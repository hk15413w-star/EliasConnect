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

    // ================== PUBLIC CHAT ==================
    public async Task SendMsg(string name, string msg)
    {
        var visitor = _tracker.GetByConnection(Context.ConnectionId);
        var deviceName = visitor?.DeviceName ?? "Unknown";
        var isAdmin = visitor?.IsAdmin ?? false;
        var finalName = isAdmin ? $"[ADMIN] {name}" : name;

        _tracker.AddMessage(finalName, deviceName, msg);
        await Clients.All.SendAsync("Msg", finalName, deviceName, msg, DateTime.Now.ToString("HH:mm"));
    }

    // ================== ADMIN COMMANDS ==================
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

    // ================== PRIVATE CHAT ==================
    // Admin bắt đầu chat riêng với 1 deviceId
    public async Task StartPrivateChat(string deviceId)
    {
        if (!_tracker.IsAdmin(Context.ConnectionId)) return;

        var adminDevice = _tracker.GetByConnection(Context.ConnectionId);
        if (adminDevice == null) return;

        // Kết thúc session cũ nếu có
        _tracker.EndPrivateSession(adminDevice.DeviceId);

        // Lưu session mới
        _tracker.StartPrivateSession(adminDevice.DeviceId, deviceId);

        // Lấy connectionId của visitor (nếu online)
        var targetConnId = _tracker.GetOnlineConnectionId(deviceId);
        if (targetConnId != null)
        {
            // Gửi cho visitor biết admin muốn chat riêng
            await Clients.Client(targetConnId).SendAsync("PrivateChatStarted", adminDevice.DeviceId, adminDevice.Nickname ?? adminDevice.DeviceName);
        }

        // Gửi cho admin lịch sử chat riêng với visitor này
        var history = _tracker.GetPrivateMessages(deviceId);
        await Clients.Caller.SendAsync("PrivateChatOpened", deviceId, history);
    }

    // Admin gửi tin nhắn trong session hiện tại
    public async Task SendPrivateMessage(string message)
    {
        var sender = _tracker.GetByConnection(Context.ConnectionId);
        if (sender == null || !sender.IsAdmin) return;

        var targetDeviceId = _tracker.GetPrivateTarget(sender.DeviceId);
        if (targetDeviceId == null) return;

        var targetConnId = _tracker.GetOnlineConnectionId(targetDeviceId);
        var senderName = $"[ADMIN] {sender.Nickname ?? sender.DeviceName}";

        // Lưu tin nhắn
        _tracker.AddPrivateMessage(targetDeviceId, senderName, message);

        // Gửi cho admin
        await Clients.Caller.SendAsync("PrivateMsg", senderName, message, DateTime.Now.ToString("HH:mm"));
        // Gửi cho visitor (nếu online)
        if (targetConnId != null)
            await Clients.Client(targetConnId).SendAsync("PrivateMsg", senderName, message, DateTime.Now.ToString("HH:mm"));
    }

    // Visitor gửi tin nhắn cho admin (chỉ khi đang trong session)
    public async Task SendPrivateMessageToAdmin(string message)
    {
        var visitor = _tracker.GetByConnection(Context.ConnectionId);
        if (visitor == null || visitor.IsAdmin) return;

        var adminDeviceId = _tracker.GetPrivateAdmin(visitor.DeviceId);
        if (adminDeviceId == null) return;

        var adminConnId = _tracker.GetOnlineConnectionId(adminDeviceId);
        var senderName = visitor.Nickname ?? visitor.DeviceName;

        // Lưu tin nhắn
        _tracker.AddPrivateMessage(visitor.DeviceId, senderName, message);

        // Gửi cho chính visitor
        await Clients.Caller.SendAsync("PrivateMsg", senderName, message, DateTime.Now.ToString("HH:mm"));
        // Gửi cho admin (nếu online)
        if (adminConnId != null)
            await Clients.Client(adminConnId).SendAsync("PrivateMsg", senderName, message, DateTime.Now.ToString("HH:mm"));
    }

    // Kết thúc session (admin hoặc visitor)
    public async Task EndPrivateChat()
    {
        var device = _tracker.GetByConnection(Context.ConnectionId);
        if (device == null) return;

        string? targetDeviceId = null;
        if (device.IsAdmin)
        {
            targetDeviceId = _tracker.GetPrivateTarget(device.DeviceId);
            _tracker.EndPrivateSession(device.DeviceId);
        }
        else
        {
            var adminDevId = _tracker.GetPrivateAdmin(device.DeviceId);
            if (adminDevId != null)
            {
                _tracker.EndPrivateSession(adminDevId);
                targetDeviceId = device.DeviceId;
            }
        }

        if (targetDeviceId != null)
        {
            var targetConnId = _tracker.GetOnlineConnectionId(targetDeviceId);
            if (targetConnId != null)
                await Clients.Client(targetConnId).SendAsync("PrivateChatEnded");
        }
        await Clients.Caller.SendAsync("PrivateChatEnded");
    }
}

public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Device> _devices = new();
    private readonly ConcurrentDictionary<string, string> _connToDevice = new();
    private readonly ConcurrentDictionary<string, string> _nicknames = new();
    private readonly ConcurrentDictionary<string, List<PrivateMessage>> _privateMessages = new();
    private readonly List<ChatMessage> _messages = new();
    private readonly object _msgLock = new();
    private readonly IHttpClientFactory _httpFactory;

    // Private session: adminDeviceId -> visitorDeviceId
    private readonly ConcurrentDictionary<string, string> _privateSessions = new();

    public VisitorTracker(IHttpClientFactory httpFactory) => _httpFactory = httpFactory;

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

        if (device.Lat == 0 && ip != "127.0.0.1" && ip != "0.0.0.0")
        {
            try
            {
                var client = _httpFactory.CreateClient();
                client.DefaultRequestHeaders.Add("User-Agent", "EliasConnect/1.0");
                var r = await client.GetStringAsync($"https://ipapi.co/{ip}/json/");
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
                device.Online = false;
        }
    }

    public Device? GetByConnection(string connectionId)
    {
        _connToDevice.TryGetValue(connectionId, out var deviceId);
        return deviceId != null && _devices.TryGetValue(deviceId, out var d) ? d : null;
    }

    public string? GetOnlineConnectionId(string deviceId)
    {
        return _connToDevice.FirstOrDefault(kvp => kvp.Value == deviceId).Key;
    }

    public bool IsAdmin(string connectionId) => GetByConnection(connectionId)?.IsAdmin ?? false;

    public void SetAdmin(string connectionId, bool isAdmin)
    {
        var d = GetByConnection(connectionId);
        if (d != null) d.IsAdmin = isAdmin;
    }

    public void SetLocation(string connectionId, double lat, double lng, string info)
    {
        if (string.IsNullOrEmpty(info)) return;
        var d = GetByConnection(connectionId);
        if (d != null) { d.Lat = lat; d.Lng = lng; d.LocationInfo = info; }
    }

    public void AddMessage(string user, string deviceName, string msg)
    {
        lock (_msgLock)
        {
            _messages.Add(new ChatMessage { User = user, DeviceName = deviceName, Message = msg, Timestamp = DateTime.Now });
            if (_messages.Count > 500) _messages.RemoveAt(0);
        }
    }

    public void AddPrivateMessage(string deviceId, string sender, string message)
    {
        var list = _privateMessages.GetOrAdd(deviceId, _ => new List<PrivateMessage>());
        lock (list)
        {
            list.Add(new PrivateMessage { Sender = sender, Message = message, Timestamp = DateTime.Now });
            if (list.Count > 200) list.RemoveAt(0);
        }
    }

    public List<PrivateMessage> GetPrivateMessages(string deviceId)
    {
        if (_privateMessages.TryGetValue(deviceId, out var list))
            lock (list) return list.ToList();
        return new List<PrivateMessage>();
    }

    public void ClearAll() { _devices.Clear(); _connToDevice.Clear(); }

    public void DeleteDevice(string deviceId)
    {
        _devices.TryRemove(deviceId, out _);
        foreach (var kv in _connToDevice.Where(x => x.Value == deviceId).ToList())
            _connToDevice.TryRemove(kv.Key, out _);
    }

    public void SetNickname(string deviceId, string nickname)
    {
        if (string.IsNullOrWhiteSpace(nickname)) _nicknames.TryRemove(deviceId, out _);
        else _nicknames[deviceId] = nickname;
    }

    public List<Device> GetAll()
    {
        return _devices.Values.OrderByDescending(d => d.LastSeen).Select(d =>
        {
            d.Nickname = _nicknames.TryGetValue(d.DeviceId, out var n) ? n : "";
            return d;
        }).ToList();
    }

    public List<ChatMessage> GetMessages() { lock (_msgLock) return _messages.ToList(); }

    // ========== Private session management ==========
    public void StartPrivateSession(string adminDeviceId, string visitorDeviceId)
    {
        _privateSessions[adminDeviceId] = visitorDeviceId;
    }

    public void EndPrivateSession(string adminDeviceId)
    {
        _privateSessions.TryRemove(adminDeviceId, out _);
    }

    public string? GetPrivateTarget(string adminDeviceId)
    {
        _privateSessions.TryGetValue(adminDeviceId, out var target);
        return target;
    }

    public string? GetPrivateAdmin(string visitorDeviceId)
    {
        return _privateSessions.FirstOrDefault(s => s.Value == visitorDeviceId).Key;
    }
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