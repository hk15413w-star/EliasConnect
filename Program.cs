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
app.MapGet("/api/visitors", (VisitorTracker tracker) => tracker.GetAll());

app.Run();

// ============================================
// CHAT HUB
// ============================================
public class ChatHub : Hub
{
    private readonly VisitorTracker _tracker;
    private static readonly HashSet<string> _admins = new();

    public ChatHub(VisitorTracker tracker)
    {
        _tracker = tracker;
    }

    private string GetIp()
    {
        var ctx = Context.GetHttpContext();
        if (ctx == null) return "Unknown";
        var forwarded = ctx.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (!string.IsNullOrEmpty(forwarded)) return forwarded;
        return ctx.Connection.RemoteIpAddress?.ToString() ?? "Unknown";
    }

    public override async Task OnConnectedAsync()
    {
        var ip = GetIp();
        var ua = Context.GetHttpContext()?.Request.Headers["User-Agent"].ToString() ?? "";
        _tracker.AddVisitor(Context.ConnectionId, ip, ua);
        await Clients.Caller.SendAsync("MyIp", ip);
        await Clients.Caller.SendAsync("ChatHistory", _tracker.GetMessages());
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex)
    {
        _tracker.RemoveVisitor(Context.ConnectionId);
        _admins.Remove(Context.ConnectionId);
        await base.OnDisconnectedAsync(ex);
    }

    public async Task SendMessage(string name, string message)
    {
        var displayName = _admins.Contains(Context.ConnectionId) ? $"[ADMIN] {name}" : name;
        _tracker.AddMessage(Context.ConnectionId, displayName, message);
        await Clients.All.SendAsync("ReceiveMessage", displayName, message, DateTime.Now.ToString("HH:mm"));
    }

    public async Task<bool> Login(string password)
    {
        if (password == "elias2026")
        {
            _admins.Add(Context.ConnectionId);
            await Clients.Caller.SendAsync("AdminLogin", _tracker.GetAll());
            return true;
        }
        return false;
    }

    public async Task RefreshVisitors()
    {
        if (_admins.Contains(Context.ConnectionId))
            await Clients.Caller.SendAsync("AdminLogin", _tracker.GetAll());
    }

    public async Task UpdateLocation(double lat, double lng, string city)
    {
        _tracker.UpdateLocation(Context.ConnectionId, lat, lng, city ?? "");
    }
}

// ============================================
// VISITOR TRACKER
// ============================================
public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Visitor> _visitors = new();
    private readonly List<ChatMessage> _messages = new();

    public void AddVisitor(string id, string ip, string ua)
    {
        _visitors[id] = new Visitor
        {
            ConnectionId = id,
            Ip = ip,
            UserAgent = ua,
            ConnectedAt = DateTime.Now
        };
    }

    public void RemoveVisitor(string id)
    {
        _visitors.TryRemove(id, out _);
    }

    public void UpdateLocation(string id, double lat, double lng, string city)
    {
        if (_visitors.TryGetValue(id, out var v))
        {
            v.Lat = lat;
            v.Lng = lng;
            v.City = city;
        }
    }

    public void AddMessage(string id, string user, string msg)
    {
        _messages.Add(new ChatMessage
        {
            ConnectionId = id,
            User = user,
            Message = msg,
            Timestamp = DateTime.Now
        });
        if (_messages.Count > 500) _messages.RemoveAt(0);
    }

    public List<Visitor> GetAll() => _visitors.Values.ToList();
    public List<ChatMessage> GetMessages() => _messages.ToList();
}

public class Visitor
{
    public string ConnectionId { get; set; } = "";
    public string Ip { get; set; } = "";
    public string UserAgent { get; set; } = "";
    public double Lat { get; set; }
    public double Lng { get; set; }
    public string City { get; set; } = "";
    public DateTime ConnectedAt { get; set; }
}

public class ChatMessage
{
    public string ConnectionId { get; set; } = "";
    public string User { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}