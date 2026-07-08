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
}

// ==================== VISITOR TRACKER ====================
public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Device> _devices = new();
    private readonly ConcurrentDictionary<string, string> _connToDevice = new();
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

        // Luôn gọi IP geolocation nếu chưa có tọa độ
        if (device.Lat == 0 && ip != "127.0.0.1" && ip != "0.0.0.0")
        {
            Console.WriteLine($"[GEO] Fetching location for IP: {ip}");
            try
            {
                var client = _httpFactory.CreateClient();
                client.DefaultRequestHeaders.Add("User-Agent", "EliasConnect/1.0");
                var url = $"https://ipapi.co/{ip}/json/";
                var r = await client.GetStringAsync(url);
                Console.WriteLine($"[GEO] Response: {r}");
                var data = JsonSerializer.Deserialize<IpapiResponse>(r);
                if (data != null && !string.IsNullOrEmpty(data.country_name))
                {
                    device.Lat = data.latitude;
                    device.Lng = data.longitude;
                    device.LocationInfo = $"{data.country_name}, {data.region}, {data.city}";
                    Console.WriteLine($"[GEO] SUCCESS: {device.LocationInfo}");
                }
                else
                {
                    Console.WriteLine("[GEO] No country in response");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[GEO] ERROR: {ex.Message}");
            }
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
}

public class ChatMessage
{
    public string User { get; set; } = "";
    public string DeviceName { get; set; } = "";
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