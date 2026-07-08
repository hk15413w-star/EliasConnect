using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
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
builder.Services.AddHttpClient();
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapHub<ChatHub>("/chat");
app.MapGet("/api/visitors", (VisitorTracker t) => t.GetAll());

app.Run();

// ==================== CHAT HUB ====================
public class ChatHub : Hub
{
    private readonly VisitorTracker _tracker;

    public ChatHub(VisitorTracker tracker) => _tracker = tracker;

    private string GetRealIp()
    {
        var ctx = Context.GetHttpContext();
        if (ctx == null) return "127.0.0.1";

        var xff = ctx.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (!string.IsNullOrEmpty(xff)) return xff.Split(',')[0].Trim();

        var xri = ctx.Request.Headers["X-Real-IP"].FirstOrDefault();
        if (!string.IsNullOrEmpty(xri)) return xri;

        var rip = ctx.Connection.RemoteIpAddress?.ToString();
        if (rip == "::1") return "127.0.0.1";
        return rip ?? "127.0.0.1";
    }

    private static string HashDevice(string ip, string ua)
    {
        var raw = ip + "|" + (ua?.Length > 60 ? ua[..60] : ua ?? "");
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(raw));
        return "Dev-" + Convert.ToHexString(hash)[..6];
    }

    public override async Task OnConnectedAsync()
    {
        var ip = GetRealIp();
        var ua = Context.GetHttpContext()?.Request.Headers["User-Agent"].ToString() ?? "";
        var deviceId = HashDevice(ip, ua);

        _tracker.Connect(Context.ConnectionId, deviceId, ip, ua);

        await Clients.Caller.SendAsync("Init", ip, deviceId);
        await Clients.Caller.SendAsync("History", _tracker.GetMessages());
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex)
    {
        _tracker.Disconnect(Context.ConnectionId);
        await base.OnDisconnectedAsync(ex);
    }

    public async Task SendMsg(string name, string msg)
    {
        var visitor = _tracker.GetByConnection(Context.ConnectionId);
        var deviceName = visitor?.DeviceName ?? "Unknown";
        var isAdmin = visitor?.IsAdmin ?? false;
        var finalName = isAdmin ? $"[ADMIN] {name}" : name;

        _tracker.AddMessage(finalName, deviceName, msg);
        await Clients.All.SendAsync("Msg", finalName, deviceName, msg, DateTime.Now.ToString("HH:mm"));
    }

    public async Task<bool> Login(string password)
    {
        if (password != "elias2026") return false;

        _tracker.SetAdmin(Context.ConnectionId, true);
        return true;
    }

    public async Task UpdateLoc(double lat, double lng, string info)
    {
        _tracker.SetLocation(Context.ConnectionId, lat, lng, info);
    }
}

// ==================== VISITOR TRACKER ====================
public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Device> _devices = new();   // deviceId -> Device
    private readonly ConcurrentDictionary<string, string> _connToDevice = new(); // connectionId -> deviceId
    private readonly List<ChatMessage> _messages = new();
    private readonly object _msgLock = new();

    public void Connect(string connectionId, string deviceId, string ip, string ua)
    {
        var device = _devices.GetOrAdd(deviceId, _ => new Device
        {
            DeviceId = deviceId,
            FirstIp = ip,
            FirstSeen = DateTime.Now,
            UserAgent = ua
        });

        device.LastIp = ip;
        device.LastSeen = DateTime.Now;
        device.Online = true;
        device.UserAgent = ua;

        _connToDevice[connectionId] = deviceId;
    }

    public void Disconnect(string connectionId)
    {
        if (_connToDevice.TryRemove(connectionId, out var deviceId))
        {
            if (_devices.TryGetValue(deviceId, out var device))
            {
                device.Online = false;
            }
        }
    }

    public Device? GetByConnection(string connectionId)
    {
        if (_connToDevice.TryGetValue(connectionId, out var deviceId))
            if (_devices.TryGetValue(deviceId, out var device))
                return device;
        return null;
    }

    public void SetAdmin(string connectionId, bool isAdmin)
    {
        var device = GetByConnection(connectionId);
        if (device != null) device.IsAdmin = isAdmin;
    }

    public void SetLocation(string connectionId, double lat, double lng, string info)
    {
        var device = GetByConnection(connectionId);
        if (device != null)
        {
            device.Lat = lat;
            device.Lng = lng;
            device.LocationInfo = info;
        }
    }

    public void AddMessage(string user, string deviceName, string msg)
    {
        lock (_msgLock)
        {
            _messages.Add(new ChatMessage
            {
                User = user,
                DeviceName = deviceName,
                Message = msg,
                Timestamp = DateTime.Now
            });
            if (_messages.Count > 500) _messages.RemoveAt(0);
        }
    }

    public List<Device> GetAll() => _devices.Values.OrderByDescending(d => d.LastSeen).ToList();
    public List<ChatMessage> GetMessages() { lock (_msgLock) return _messages.ToList(); }
}

public class Device
{
    public string DeviceId { get; set; } = "";
    public string FirstIp { get; set; } = "";
    public string LastIp { get; set; } = "";
    public string UserAgent { get; set; } = "";
    public double Lat { get; set; }
    public double Lng { get; set; }
    public string LocationInfo { get; set; } = "";
    public bool Online { get; set; }
    public bool IsAdmin { get; set; }
    public DateTime FirstSeen { get; set; }
    public DateTime LastSeen { get; set; }
    public string DeviceName => DeviceId;
}

public class ChatMessage
{
    public string User { get; set; } = "";
    public string DeviceName { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}