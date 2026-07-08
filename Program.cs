using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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

app.Run();

// ============================================
// CHAT HUB — XỬ LÝ TIN NHẮN THỜI GIAN THỰC
// ============================================
public class ChatHub : Hub
{
    private readonly VisitorTracker _tracker;

    public ChatHub(VisitorTracker tracker)
    {
        _tracker = tracker;
    }

    public override async Task OnConnectedAsync()
    {
        var connectionId = Context.ConnectionId;
        var ip = Context.GetHttpContext()?.Connection.RemoteIpAddress?.ToString() ?? "Unknown";
        _tracker.AddVisitor(connectionId, ip);
        await Clients.All.SendAsync("UserConnected", connectionId);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _tracker.RemoveVisitor(Context.ConnectionId);
        await Clients.All.SendAsync("UserDisconnected", Context.ConnectionId);
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
        await Clients.All.SendAsync("LocationUpdated", connectionId, latitude, longitude, city);
    }

    public async Task GetChatHistory()
    {
        var history = _tracker.GetMessages();
        await Clients.Caller.SendAsync("ChatHistory", history);
    }
}

// ============================================
// VISITOR TRACKER — LƯU VỊ TRÍ & TIN NHẮN
// ============================================
public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Visitor> _visitors = new();
    private readonly List<ChatMessage> _messages = new();
    private readonly string _logPath = "visitors_log.json";

    public void AddVisitor(string connectionId, string ip)
    {
        var visitor = new Visitor
        {
            ConnectionId = connectionId,
            IpAddress = ip,
            ConnectedAt = DateTime.Now,
            UserAgent = "Unknown"
        };
        _visitors[connectionId] = visitor;
        SaveLog();
    }

    public void RemoveVisitor(string connectionId)
    {
        _visitors.TryRemove(connectionId, out _);
        SaveLog();
    }

    public void UpdateLocation(string connectionId, double lat, double lng, string city)
    {
        if (_visitors.TryGetValue(connectionId, out var visitor))
        {
            visitor.Latitude = lat;
            visitor.Longitude = lng;
            visitor.City = city;
            SaveLog();
        }
    }

    public void AddMessage(string connectionId, string user, string message)
    {
        var msg = new ChatMessage
        {
            ConnectionId = connectionId,
            User = user,
            Message = message,
            Timestamp = DateTime.Now
        };
        _messages.Add(msg);
        if (_messages.Count > 500) _messages.RemoveAt(0);
    }

    public List<ChatMessage> GetMessages() => _messages;

    public List<Visitor> GetAll()
    {
        return new List<Visitor>(_visitors.Values);
    }

    public List<Visitor> GetLocations()
    {
        return new List<Visitor>(_visitors.Values)
            .FindAll(v => v.Latitude != 0 || v.Longitude != 0);
    }

    private void SaveLog()
    {
        try
        {
            var data = new { Visitors = _visitors.Values, Messages = _messages };
            File.WriteAllText(_logPath, JsonSerializer.Serialize(data, new JsonSerializerOptions { WriteIndented = true }));
        }
        catch { }
    }
}

public class Visitor
{
    public string ConnectionId { get; set; } = "";
    public string IpAddress { get; set; } = "";
    public string UserAgent { get; set; } = "";
    public double Latitude { get; set; }
    public double Longitude { get; set; }
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