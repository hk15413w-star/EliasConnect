using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSignalR();
builder.Services.AddSingleton<VisitorTracker>();
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapHub<ChatHub>("/chat");
app.MapGet("/api/visitors", (VisitorTracker t) => Results.Ok(t.GetAll()));

app.Run();

public class ChatHub : Hub
{
    private readonly VisitorTracker _tracker;
    private static readonly HashSet<string> _adminIds = new();
    private static readonly object _lock = new();

    public ChatHub(VisitorTracker tracker) => _tracker = tracker;

    private string GetIp()
    {
        var ctx = Context.GetHttpContext();
        if (ctx == null) return "0.0.0.0";
        var fwd = ctx.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (!string.IsNullOrEmpty(fwd)) return fwd.Split(',')[0].Trim();
        var ip = ctx.Connection.RemoteIpAddress?.ToString();
        if (ip == "::1") return "127.0.0.1";
        return ip ?? "0.0.0.0";
    }

    private string HashDevice(string ip, string ua)
    {
        var raw = ip + "|" + (ua?.Length > 50 ? ua[..50] : ua ?? "");
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(raw));
        return "Device-" + Convert.ToHexString(hash)[..6];
    }

    public override async Task OnConnectedAsync()
    {
        var ip = GetIp();
        var ua = Context.GetHttpContext()?.Request.Headers["User-Agent"].ToString() ?? "";
        var deviceId = HashDevice(ip, ua);
        var cid = Context.ConnectionId;

        _tracker.Register(cid, deviceId, ip, ua);

        await Clients.Caller.SendAsync("Init", ip, deviceId);
        await Clients.Caller.SendAsync("History", _tracker.GetMessages());

        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex)
    {
        lock (_lock) { _adminIds.Remove(Context.ConnectionId); }
        _tracker.SetOffline(Context.ConnectionId);
        await NotifyAdmins();
        await base.OnDisconnectedAsync(ex);
    }

    public async Task SendMsg(string displayName, string message)
    {
        var v = _tracker.Get(Context.ConnectionId);
        var deviceName = v?.DeviceName ?? "Unknown";
        var isAdmin = _adminIds.Contains(Context.ConnectionId);
        var finalName = isAdmin ? "[ADMIN] " + displayName : displayName;
        _tracker.AddMessage(Context.ConnectionId, finalName, deviceName, message);
        await Clients.All.SendAsync("Msg", finalName, deviceName, message, DateTime.Now.ToString("HH:mm"));
    }

    public async Task<bool> Login(string password)
    {
        if (password == "elias2026")
        {
            lock (_lock) { _adminIds.Add(Context.ConnectionId); }
            await NotifyAdmins();
            return true;
        }
        return false;
    }

    public async Task UpdateLoc(double lat, double lng, string info)
    {
        _tracker.SetLocation(Context.ConnectionId, lat, lng, info);
        await NotifyAdmins();
    }

    private async Task NotifyAdmins()
    {
        var all = _tracker.GetAll();
        List<string> admins;
        lock (_lock) { admins = _adminIds.ToList(); }

        foreach (var adminId in admins)
        {
            try
            {
                await Clients.Client(adminId).SendAsync("AdminData", all);
            }
            catch
            {
                lock (_lock) { _adminIds.Remove(adminId); }
            }
        }
    }
}

public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Visitor> _visitors = new();

    public void Register(string cid, string deviceId, string ip, string ua)
    {
        var existing = _visitors.Values.FirstOrDefault(v => v.DeviceName == deviceId);
        if (existing != null)
        {
            existing.ConnectionId = cid;
            existing.Ip = ip;
            existing.Online = true;
            existing.LastSeen = DateTime.Now;
            _visitors.TryRemove(existing.ConnectionId, out _);
            _visitors[cid] = existing;
        }
        else
        {
            _visitors[cid] = new Visitor
            {
                ConnectionId = cid,
                DeviceName = deviceId,
                Ip = ip,
                UserAgent = ua,
                Online = true,
                FirstSeen = DateTime.Now,
                LastSeen = DateTime.Now
            };
        }
    }

    public Visitor? Get(string cid)
    {
        _visitors.TryGetValue(cid, out var v);
        return v;
    }

    public void SetOffline(string cid)
    {
        if (_visitors.TryGetValue(cid, out var v))
            v.Online = false;
    }

    public void SetLocation(string cid, double lat, double lng, string info)
    {
        if (_visitors.TryGetValue(cid, out var v))
        {
            v.Lat = lat;
            v.Lng = lng;
            v.LocationInfo = info;
        }
    }

    private readonly List<ChatMessage> _messages = new();

    public void AddMessage(string cid, string user, string device, string msg)
    {
        _messages.Add(new ChatMessage
        {
            User = user,
            DeviceName = device,
            Message = msg,
            Timestamp = DateTime.Now
        });
        if (_messages.Count > 500) _messages.RemoveAt(0);
    }

    public List<Visitor> GetAll() => _visitors.Values.OrderByDescending(v => v.LastSeen).ToList();
    public List<ChatMessage> GetMessages() => _messages.ToList();
}

public class Visitor
{
    public string ConnectionId { get; set; } = "";
    public string DeviceName { get; set; } = "";
    public string Ip { get; set; } = "";
    public string UserAgent { get; set; } = "";
    public double Lat { get; set; }
    public double Lng { get; set; }
    public string LocationInfo { get; set; } = "";
    public bool Online { get; set; }
    public DateTime FirstSeen { get; set; }
    public DateTime LastSeen { get; set; }
}

public class ChatMessage
{
    public string User { get; set; } = "";
    public string DeviceName { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}