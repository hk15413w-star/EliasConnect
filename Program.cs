using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
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
app.MapGet("/api/locations", (VisitorTracker tracker) => tracker.GetLocations());
app.MapGet("/api/my-ip", (HttpContext context) =>
{
    var ip = context.Request.Headers["X-Forwarded-For"].FirstOrDefault()
          ?? context.Connection.RemoteIpAddress?.ToString()
          ?? "Unknown";
    return Results.Ok(new { ip });
});

app.Run();

// ============================================
// CHAT HUB
// ============================================
public class ChatHub : Hub
{
    private readonly VisitorTracker _tracker;
    private static readonly HashSet<string> _admins = new() { "admin" };
    private static readonly HashSet<string> _authorizedViewers = new();

    public ChatHub(VisitorTracker tracker)
    {
        _tracker = tracker;
    }

    private string GetIp()
    {
        var ctx = Context.GetHttpContext();
        return ctx?.Request.Headers["X-Forwarded-For"].FirstOrDefault()
            ?? ctx?.Connection.RemoteIpAddress?.ToString()
            ?? "Unknown";
    }

    public override async Task OnConnectedAsync()
    {
        var connectionId = Context.ConnectionId;
        var ip = GetIp();
        var userAgent = Context.GetHttpContext()?.Request.Headers["User-Agent"].ToString() ?? "";
        _tracker.AddVisitor(connectionId, ip, userAgent);
        await Clients.Caller.SendAsync("SetConnectionId", connectionId);
        await Clients.All.SendAsync("VisitorCountUpdated", _tracker.Count());
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _tracker.RemoveVisitor(Context.ConnectionId);
        await Clients.All.SendAsync("VisitorCountUpdated", _tracker.Count());
        await base.OnDisconnectedAsync(exception);
    }

    public async Task SendMessage(string user, string message)
    {
        var connectionId = Context.ConnectionId;
        _tracker.AddMessage(connectionId, user, message);
        await Clients.All.SendAsync("ReceiveMessage", user, message, DateTime.Now.ToString("HH:mm"));
    }

    public async Task UpdateLocation(double latitude, double longitude, string city)
    {
        var connectionId = Context.ConnectionId;
        _tracker.UpdateLocation(connectionId, latitude, longitude, city);
        await Clients.All.SendAsync("LocationUpdated");
    }

    public async Task GetChatHistory()
    {
        var history = _tracker.GetMessages();
        await Clients.Caller.SendAsync("ChatHistory", history);
    }

    public async Task<bool> LoginAsAdmin(string password)
    {
        if (password == "elias2026")
        {
            _admins.Add(Context.ConnectionId);
            await Clients.Caller.SendAsync("AdminStatusChanged", true);
            await UpdateVisitorListForCaller();
            return true;
        }
        return false;
    }

    public async Task AddAuthorizedViewer(string connectionId)
    {
        if (_admins.Contains(Context.ConnectionId))
        {
            _authorizedViewers.Add(connectionId);
            await Clients.Client(connectionId).SendAsync("AuthorizedStatusChanged", true);
            await Clients.Client(connectionId).SendAsync("VisitorList", _tracker.GetLocations());
        }
    }

    public async Task RemoveAuthorizedViewer(string connectionId)
    {
        if (_admins.Contains(Context.ConnectionId))
        {
            _authorizedViewers.Remove(connectionId);
            await Clients.Client(connectionId).SendAsync("AuthorizedStatusChanged", false);
        }
    }

    public async Task RequestVisitorList()
    {
        if (_admins.Contains(Context.ConnectionId) || _authorizedViewers.Contains(Context.ConnectionId))
        {
            await UpdateVisitorListForCaller();
        }
    }

    public Task<bool> IsAuthorized()
    {
        return Task.FromResult(_admins.Contains(Context.ConnectionId) || _authorizedViewers.Contains(Context.ConnectionId));
    }

    private async Task UpdateVisitorListForCaller()
    {
        var visitors = _tracker.GetLocations();
        await Clients.Caller.SendAsync("VisitorList", visitors);
    }
}

// ============================================
// VISITOR TRACKER
// ============================================
public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Visitor> _visitors = new();
    private readonly List<ChatMessage> _messages = new();

    public void AddVisitor(string connectionId, string ip, string userAgent)
    {
        _visitors[connectionId] = new Visitor
        {
            ConnectionId = connectionId,
            IpAddress = ip,
            UserAgent = userAgent,
            ConnectedAt = DateTime.Now
        };
    }

    public void RemoveVisitor(string connectionId)
    {
        _visitors.TryRemove(connectionId, out _);
    }

    public void UpdateLocation(string connectionId, double lat, double lng, string city)
    {
        if (_visitors.TryGetValue(connectionId, out var v))
        {
            v.Latitude = lat;
            v.Longitude = lng;
            v.City = city;
        }
    }

    public void AddMessage(string connectionId, string user, string message)
    {
        _messages.Add(new ChatMessage
        {
            ConnectionId = connectionId,
            User = user,
            Message = message,
            Timestamp = DateTime.Now
        });
        if (_messages.Count > 500) _messages.RemoveAt(0);
    }

    public int Count() => _visitors.Count;
    public List<ChatMessage> GetMessages() => new(_messages);
    public List<Visitor> GetAll() => new(_visitors.Values);
    public List<Visitor> GetLocations() => new List<Visitor>(_visitors.Values).FindAll(v => v.IpAddress != "Unknown");
}

public class Visitor
{
    public string ConnectionId { get; set; } = "";
    public string IpAddress { get; set; } = "";
    public string UserAgent { get; set; } = "";
    public double Latitude { get; set; }
    public double Longitude { get; set; }
    public string City { get; set; } = "Unknown";
    public DateTime ConnectedAt { get; set; }
}

public class ChatMessage
{
    public string ConnectionId { get; set; } = "";
    public string User { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}