using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
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
app.MapGet("/api/ipinfo", async (HttpContext ctx, IHttpClientFactory hf) =>
{
    var ip = ctx.Request.Headers["X-Forwarded-For"].FirstOrDefault()
          ?? ctx.Connection.RemoteIpAddress?.ToString() ?? "";
    if (string.IsNullOrEmpty(ip) || ip == "::1" || ip == "127.0.0.1") return Results.Ok(new { status = "local" });
    try
    {
        var c = hf.CreateClient();
        var r = await c.GetStringAsync($"http://ip-api.com/json/{ip}?fields=country,regionName,city,lat,lon");
        return Results.Content(r, "application/json");
    }
    catch { return Results.Ok(new { status = "error" }); }
});

app.Run();

public class ChatHub : Hub
{
    private readonly VisitorTracker _t;
    private static readonly HashSet<string> _admins = new();

    public ChatHub(VisitorTracker t) => _t = t;

    private string GetIp()
    {
        var ctx = Context.GetHttpContext();
        if (ctx == null) return "Unknown";
        var fwd = ctx.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        return !string.IsNullOrEmpty(fwd) ? fwd : ctx.Connection.RemoteIpAddress?.ToString() ?? "Unknown";
    }

    public override async Task OnConnectedAsync()
    {
        var ip = GetIp();
        var ua = Context.GetHttpContext()?.Request.Headers["User-Agent"].ToString() ?? "";
        var deviceId = _t.AddOrUpdate(Context.ConnectionId, ip, ua);
        await Clients.Caller.SendAsync("Init", ip, deviceId);
        await Clients.Caller.SendAsync("History", _t.GetMessages());
        if (_admins.Contains(Context.ConnectionId))
            await Clients.Caller.SendAsync("AdminData", _t.GetAll());
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex)
    {
        _t.Remove(Context.ConnectionId);
        _admins.Remove(Context.ConnectionId);
        await base.OnDisconnectedAsync(ex);
    }

    public async Task SendMsg(string displayName, string message)
    {
        var v = _t.Get(Context.ConnectionId);
        var deviceName = v?.DeviceName ?? "Guest-" + Context.ConnectionId[..4];
        var isAdmin = _admins.Contains(Context.ConnectionId);
        var finalName = isAdmin ? $"[ADMIN] {displayName}" : displayName;
        _t.AddMessage(Context.ConnectionId, finalName, deviceName, message);
        await Clients.All.SendAsync("Msg", finalName, deviceName, message, DateTime.Now.ToString("HH:mm"));
    }

    public async Task<bool> Login(string password)
    {
        if (password == "elias2026")
        {
            _admins.Add(Context.ConnectionId);
            await Clients.Caller.SendAsync("AdminData", _t.GetAll());
            return true;
        }
        return false;
    }

    public async Task Refresh()
    {
        if (_admins.Contains(Context.ConnectionId))
            await Clients.Caller.SendAsync("AdminData", _t.GetAll());
    }

    public async Task UpdateLoc(double lat, double lng, string info)
    {
        _t.UpdateLocation(Context.ConnectionId, lat, lng, info);
        if (_admins.Any())
        {
            var all = _t.GetAll();
            foreach (var a in _admins)
                await Clients.Client(a).SendAsync("AdminData", all);
        }
    }
}

public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Visitor> _v = new();
    private readonly List<ChatMessage> _m = new();
    private int _counter;

    public string AddOrUpdate(string id, string ip, string ua)
    {
        if (_v.TryGetValue(id, out var existing))
        {
            existing.Ip = ip;
            existing.ConnectedAt = DateTime.Now;
            return existing.DeviceName;
        }
        _counter++;
        var name = "Device-" + _counter;
        _v[id] = new Visitor
        {
            ConnectionId = id,
            DeviceName = name,
            Ip = ip,
            UserAgent = ua,
            ConnectedAt = DateTime.Now
        };
        return name;
    }

    public Visitor? Get(string id) => _v.TryGetValue(id, out var v) ? v : null;

    public void Remove(string id) => _v.TryRemove(id, out _);

    public void UpdateLocation(string id, double lat, double lng, string info)
    {
        if (_v.TryGetValue(id, out var v))
        {
            v.Lat = lat;
            v.Lng = lng;
            v.LocationInfo = info;
        }
    }

    public void AddMessage(string id, string displayName, string deviceName, string msg)
    {
        _m.Add(new ChatMessage
        {
            ConnectionId = id,
            User = displayName,
            DeviceName = deviceName,
            Message = msg,
            Timestamp = DateTime.Now
        });
        if (_m.Count > 500) _m.RemoveAt(0);
    }

    public List<Visitor> GetAll() => _v.Values.OrderByDescending(x => x.ConnectedAt).ToList();
    public List<ChatMessage> GetMessages() => _m.ToList();
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
    public DateTime ConnectedAt { get; set; }
}

public class ChatMessage
{
    public string ConnectionId { get; set; } = "";
    public string User { get; set; } = "";
    public string DeviceName { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}