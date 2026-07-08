using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
app.MapGet("/api/visitors", (VisitorTracker t) => t.GetAll());

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
        var deviceId = _t.Register(Context.ConnectionId, ip, ua);
        await Clients.Caller.SendAsync("Init", ip, deviceId);
        await Clients.Caller.SendAsync("History", _t.GetMessages());
        if (_admins.Contains(Context.ConnectionId))
            await Clients.Caller.SendAsync("AdminData", _t.GetAll());
        await BroadcastToAdmins();
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex)
    {
        _t.Disconnect(Context.ConnectionId);
        _admins.Remove(Context.ConnectionId);
        await BroadcastToAdmins();
        await base.OnDisconnectedAsync(ex);
    }

    public async Task SendMsg(string displayName, string message)
    {
        var v = _t.GetByConnection(Context.ConnectionId);
        var deviceName = v?.DeviceName ?? "Unknown";
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
        await BroadcastToAdmins();
    }

    private async Task BroadcastToAdmins()
    {
        var all = _t.GetAll();
        foreach (var a in _admins)
            await Clients.Client(a).SendAsync("AdminData", all);
    }
}

public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Visitor> _connections = new(); // connectionId -> visitor
    private readonly ConcurrentDictionary<string, string> _deviceMap = new(); // deviceId -> connectionId
    private readonly List<ChatMessage> _messages = new();
    private int _counter;

    public string Register(string connectionId, string ip, string ua)
    {
        // Tạo deviceId dựa trên IP + UserAgent (định danh thiết bị)
        var deviceId = Convert.ToBase64String(
            System.Security.Cryptography.SHA256.HashData(
                System.Text.Encoding.UTF8.GetBytes(ip + "|" + ua)
            )
        )[..12];

        if (_deviceMap.TryGetValue(deviceId, out var existingConn))
        {
            // Thiết bị đã có -> giữ nguyên DeviceName, cập nhật connection mới
            if (_connections.TryGetValue(existingConn, out var existingVisitor))
            {
                existingVisitor.ConnectionId = connectionId;
                existingVisitor.Ip = ip;
                existingVisitor.Online = true;
                existingVisitor.ConnectedAt = DateTime.Now;
                _connections.TryRemove(existingConn, out _);
                _connections[connectionId] = existingVisitor;
                _deviceMap[deviceId] = connectionId;
                return existingVisitor.DeviceName;
            }
        }

        // Thiết bị mới
        _counter++;
        var name = "Device-" + _counter;
        var visitor = new Visitor
        {
            ConnectionId = connectionId,
            DeviceName = name,
            Ip = ip,
            UserAgent = ua,
            Online = true,
            ConnectedAt = DateTime.Now
        };
        _connections[connectionId] = visitor;
        _deviceMap[deviceId] = connectionId;
        return name;
    }

    public void Disconnect(string connectionId)
    {
        if (_connections.TryGetValue(connectionId, out var v))
            v.Online = false;
    }

    public Visitor? GetByConnection(string connectionId)
    {
        _connections.TryGetValue(connectionId, out var v);
        return v;
    }

    public void UpdateLocation(string connectionId, double lat, double lng, string info)
    {
        if (_connections.TryGetValue(connectionId, out var v))
        {
            v.Lat = lat;
            v.Lng = lng;
            v.LocationInfo = info;
        }
    }

    public void AddMessage(string id, string displayName, string deviceName, string msg)
    {
        _messages.Add(new ChatMessage
        {
            ConnectionId = id,
            User = displayName,
            DeviceName = deviceName,
            Message = msg,
            Timestamp = DateTime.Now
        });
        if (_messages.Count > 500) _messages.RemoveAt(0);
    }

    public List<Visitor> GetAll() => _connections.Values.OrderByDescending(x => x.ConnectedAt).ToList();
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