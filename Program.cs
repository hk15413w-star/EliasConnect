using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
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

app.Run();

// ==================== CHAT HUB ====================
public class ChatHub : Hub
{
    private readonly VisitorTracker _tracker;

    public ChatHub(VisitorTracker tracker) => _tracker = tracker;

    private string GetRealIp()
    {
        var ctx = Context.GetHttpContext();
        if (ctx == null) return "0.0.0.0";

        var xff = ctx.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (!string.IsNullOrEmpty(xff))
        {
            var ips = xff.Split(',').Select(i => i.Trim());
            foreach (var ip in ips)
            {
                if (!ip.StartsWith("10.") && !ip.StartsWith("172.") && !ip.StartsWith("192.168."))
                    return ip;
            }
            return ips.First();
        }

        var xri = ctx.Request.Headers["X-Real-IP"].FirstOrDefault();
        if (!string.IsNullOrEmpty(xri)) return xri;

        var rip = ctx.Connection.RemoteIpAddress?.ToString();
        if (rip == "::1") return "127.0.0.1";
        return rip ?? "0.0.0.0";
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

        await _tracker.ConnectAsync(Context.ConnectionId, deviceId, ip, ua);

        await Clients.Caller.SendAsync("Init", ip, deviceId);
        await Clients.Caller.SendAsync("History", _tracker.GetMessages());
        await Clients.Caller.SendAsync("PrivateHistory", _tracker.GetPrivateMessages(deviceId));
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex)
    {
        _tracker.Disconnect(Context.ConnectionId);
        await base.OnDisconnectedAsync(ex);
    }

    // --- Public Chat ---
    public async Task SendMsg(string name, string msg)
    {
        var visitor = _tracker.GetByConnection(Context.ConnectionId);
        var deviceName = visitor?.DeviceName ?? "Unknown";
        var isAdmin = visitor?.IsAdmin ?? false;
        var finalName = isAdmin ? $"[ADMIN] {name}" : name;

        _tracker.AddMessage(finalName, deviceName, msg);
        await Clients.All.SendAsync("Msg", finalName, deviceName, msg, DateTime.Now.ToString("HH:mm"));
    }

    // --- Admin ---
    public Task<bool> Login(string password)
    {
        if (password != "elias2026") return Task.FromResult(false);
        _tracker.SetAdmin(Context.ConnectionId, true);
        return Task.FromResult(true);
    }

    public Task UpdateLoc(double lat, double lng, string info)
    {
        _tracker.SetLocation(Context.ConnectionId, lat, lng, info);
        return Task.CompletedTask;
    }

    public async Task ClearAll()
    {
        if (!_tracker.IsAdmin(Context.ConnectionId)) return;
        _tracker.ClearAll();
        await Clients.Caller.SendAsync("AdminData", _tracker.GetAll());
    }

    public async Task DeleteVisitor(string deviceId)
    {
        if (!_tracker.IsAdmin(Context.ConnectionId)) return;
        _tracker.DeleteDevice(deviceId);
        await Clients.Caller.SendAsync("AdminData", _tracker.GetAll());
    }

    public async Task SetNickname(string deviceId, string nickname)
    {
        if (!_tracker.IsAdmin(Context.ConnectionId)) return;
        _tracker.SetNickname(deviceId, nickname);
        await Clients.Caller.SendAsync("AdminData", _tracker.GetAll());
    }

    // --- Private Chat (Admin ↔ Visitor) ---
    public async Task SendPrivateMessage(string targetDeviceId, string message)
    {
        var sender = _tracker.GetByConnection(Context.ConnectionId);
        if (sender == null || !sender.IsAdmin) return; // chỉ admin được gửi riêng

        var targetConnId = _tracker.GetOnlineConnectionId(targetDeviceId);
        if (targetConnId == null) return; // visitor không online

        var senderName = $"[ADMIN] {sender.DeviceName}";
        _tracker.AddPrivateMessage(targetDeviceId, senderName, message);

        // Gửi cho admin
        await Clients.Caller.SendAsync("PrivateMsg", targetDeviceId, senderName, message, DateTime.Now.ToString("HH:mm"));
        // Gửi cho visitor
        await Clients.Client(targetConnId).SendAsync("PrivateMsg", targetDeviceId, senderName, message, DateTime.Now.ToString("HH:mm"));
    }

    public async Task GetPrivateHistory(string deviceId)
    {
        var visitor = _tracker.GetByConnection(Context.ConnectionId);
        if (visitor == null) return;
        // Chỉ admin hoặc chính visitor đó mới xem được lịch sử riêng
        if (visitor.IsAdmin || visitor.DeviceId == deviceId)
        {
            var msgs = _tracker.GetPrivateMessages(deviceId);
            await Clients.Caller.SendAsync("PrivateHistory", msgs);
        }
    }
}

// ==================== VISITOR TRACKER ====================
public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Device> _devices = new();
    private readonly ConcurrentDictionary<string, string> _connToDevice = new();
    private readonly ConcurrentDictionary<string, string> _nicknames = new();
    private readonly ConcurrentDictionary<string, List<PrivateMessage>> _privateMessages = new(); // deviceId -> messages
    private readonly List<ChatMessage> _messages = new();
    private readonly object _msgLock = new();
    private readonly IHttpClientFactory _httpFactory;

    public VisitorTracker(IHttpClientFactory httpFactory)
    {
        _httpFactory = httpFactory;
    }

    public async Task ConnectAsync(string connectionId, string deviceId, string ip, string ua)
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

        // IP geolocation tự động (không cần GPS permission)
        if (device.Lat == 0 && ip != "127.0.0.1" && ip != "0.0.0.0")
        {
            Console.WriteLine($"[GEO] Fetching location for IP: {ip}");
            try
            {
                var client = _httpFactory.CreateClient();
                client.DefaultRequestHeaders.Add("User-Agent", "EliasConnect/1.0");
                var url = $"https://ipapi.co/{ip}/json/";
                var r = await client.GetStringAsync(url);
                var data = JsonSerializer.Deserialize<IpapiResponse>(r);
                if (data != null && !string.IsNullOrEmpty(data.country_name))
                {
                    device.Lat = data.latitude;
                    device.Lng = data.longitude;
                    device.LocationInfo = $"{data.country_name}, {data.region}, {data.city}";
                }
            }
            catch { }
        }
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

    public string? GetOnlineConnectionId(string deviceId)
    {
        // Tìm connectionId đang online của device
        return _connToDevice.FirstOrDefault(kvp => kvp.Value == deviceId).Key;
    }

    public bool IsAdmin(string connectionId) => GetByConnection(connectionId)?.IsAdmin ?? false;

    public void SetAdmin(string connectionId, bool isAdmin)
    {
        var device = GetByConnection(connectionId);
        if (device != null) device.IsAdmin = isAdmin;
    }

    public void SetLocation(string connectionId, double lat, double lng, string info)
    {
        if (string.IsNullOrEmpty(info)) return;
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
            _messages.Add(new ChatMessage { User = user, DeviceName = deviceName, Message = msg, Timestamp = DateTime.Now });
            if (_messages.Count > 500) _messages.RemoveAt(0);
        }
    }

    public void AddPrivateMessage(string deviceId, string senderName, string message)
    {
        var list = _privateMessages.GetOrAdd(deviceId, _ => new List<PrivateMessage>());
        lock (list)
        {
            list.Add(new PrivateMessage { Sender = senderName, Message = message, Timestamp = DateTime.Now });
            if (list.Count > 200) list.RemoveAt(0);
        }
    }

    public List<PrivateMessage> GetPrivateMessages(string deviceId)
    {
        if (_privateMessages.TryGetValue(deviceId, out var list))
        {
            lock (list) return list.ToList();
        }
        return new List<PrivateMessage>();
    }

    public void ClearAll()
    {
        _devices.Clear();
        _connToDevice.Clear();
    }

    public void DeleteDevice(string deviceId)
    {
        _devices.TryRemove(deviceId, out _);
        // Xóa connection tương ứng nếu có
        var connToRemove = _connToDevice.Where(kvp => kvp.Value == deviceId).Select(kvp => kvp.Key).ToList();
        foreach (var conn in connToRemove) _connToDevice.TryRemove(conn, out _);
    }

    public void SetNickname(string deviceId, string nickname)
    {
        if (string.IsNullOrWhiteSpace(nickname))
            _nicknames.TryRemove(deviceId, out _);
        else
            _nicknames[deviceId] = nickname;
    }

    public List<Device> GetAll()
    {
        return _devices.Values.OrderByDescending(d => d.LastSeen).Select(d =>
        {
            d.Nickname = _nicknames.TryGetValue(d.DeviceId, out var nick) ? nick : "";
            return d;
        }).ToList();
    }

    public List<ChatMessage> GetMessages() { lock (_msgLock) return _messages.ToList(); }
}

// ==================== MODELS ====================
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
    public string Nickname { get; set; } = "";
}

public class ChatMessage
{
    public string User { get; set; } = "";
    public string DeviceName { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}

public class PrivateMessage
{
    public string Sender { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}

public class IpapiResponse
{
    public string country_name { get; set; } = "";
    public string region { get; set; } = "";
    public string city { get; set; } = "";
    public double latitude { get; set; }
    public double longitude { get; set; }
}