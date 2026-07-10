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
    private readonly VisitorTracker _t;
    public ChatHub(VisitorTracker t) => _t = t;

    private string GetRealIp()
    {
        var ctx = Context.GetHttpContext();
        if (ctx == null) return "0.0.0.0";
        var xff = ctx.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (!string.IsNullOrEmpty(xff))
        {
            var ips = xff.Split(',').Select(i => i.Trim());
            foreach (var ip in ips)
                if (!ip.StartsWith("10.") && !ip.StartsWith("172.") && !ip.StartsWith("192.168.")) return ip;
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
        var ip = GetRealIp(); var ua = Context.GetHttpContext()?.Request.Headers["User-Agent"].ToString() ?? "";
        var did = HashDevice(ip, ua);
        await _t.ConnectAsync(Context.ConnectionId, did, ip, ua);
        await Clients.Caller.SendAsync("Init", ip, did);
        await Clients.Caller.SendAsync("History", _t.GetMessages());
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? ex) { _t.Disconnect(Context.ConnectionId); await base.OnDisconnectedAsync(ex); }
    public async Task SendMsg(string name, string msg, bool save, string deviceId) { var d = _t.GetByConnection(Context.ConnectionId); var dn = d?.DisplayName ?? "Unknown"; var adm = d?.IsAdmin ?? false; var fn = adm ? $"[ADMIN] {name}" : name; _t.AddMessage(fn, dn, msg, save); await Clients.All.SendAsync("Msg", fn, dn, msg, DateTime.Now.ToString("HH:mm"), deviceId); }
    public Task<bool> Login(string pw) { if (pw != "elias2026") return Task.FromResult(false); _t.SetAdmin(Context.ConnectionId, true); return Task.FromResult(true); }
    public Task UpdateLoc(double lat, double lng, string info) { _t.SetLocation(Context.ConnectionId, lat, lng, info); return Task.CompletedTask; }
    public async Task ClearAll() { if (!_t.IsAdmin(Context.ConnectionId)) return; _t.ClearAll(); await Clients.Caller.SendAsync("AdminData", _t.GetAll()); }
    public async Task DeleteVisitor(string deviceId) { if (!_t.IsAdmin(Context.ConnectionId)) return; _t.DeleteDevice(deviceId); await Clients.Caller.SendAsync("AdminData", _t.GetAll()); }
    public async Task SetNickname(string deviceId, string nick) { if (!_t.IsAdmin(Context.ConnectionId)) return; _t.SetNickname(deviceId, nick); await Clients.Caller.SendAsync("AdminData", _t.GetAll()); }
    public async Task AdminStartPrivate(string targetDeviceId) { if (!_t.IsAdmin(Context.ConnectionId)) return; var adm = _t.GetByConnection(Context.ConnectionId); if (adm == null) return; _t.StartPrivateSession(adm.DeviceId, targetDeviceId); var tc = _t.GetOnlineConnectionId(targetDeviceId); if (tc != null) await Clients.Client(tc).SendAsync("AdminStartedChat", adm.DisplayName); await Clients.Caller.SendAsync("PrivateOpened", targetDeviceId, _t.GetPrivateMessages(adm.DeviceId, targetDeviceId)); }
    public async Task AdminSendPrivate(string tid, string msg) { var s = _t.GetByConnection(Context.ConnectionId); if (s == null || !s.IsAdmin) return; var sn = $"[ADMIN] {s.DisplayName}"; _t.AddPrivateMessage(s.DeviceId, tid, sn, msg); await Clients.Caller.SendAsync("PrivateMsg", tid, sn, msg, DateTime.Now.ToString("HH:mm")); var tc = _t.GetOnlineConnectionId(tid); if (tc != null) await Clients.Client(tc).SendAsync("PrivateMsg", s.DeviceId, sn, msg, DateTime.Now.ToString("HH:mm")); }
    public async Task VisitorSendPrivate(string msg) { var v = _t.GetByConnection(Context.ConnectionId); if (v == null || v.IsAdmin) return; var aid = _t.GetPrivateAdmin(v.DeviceId); if (aid == null) return; var sn = v.DisplayName; _t.AddPrivateMessage(aid, v.DeviceId, sn, msg); await Clients.Caller.SendAsync("PrivateMsg", aid, sn, msg, DateTime.Now.ToString("HH:mm")); var ac = _t.GetOnlineConnectionId(aid); if (ac != null) await Clients.Client(ac).SendAsync("PrivateMsg", v.DeviceId, sn, msg, DateTime.Now.ToString("HH:mm")); }
    public async Task EndPrivate(string tid) { var d = _t.GetByConnection(Context.ConnectionId); if (d == null) return; if (d.IsAdmin) { _t.EndPrivateSession(d.DeviceId); var tc = _t.GetOnlineConnectionId(tid); if (tc != null) await Clients.Client(tc).SendAsync("PrivateEnded"); } else { var aid = _t.GetPrivateAdmin(d.DeviceId); if (aid != null) { _t.EndPrivateSession(aid); var ac = _t.GetOnlineConnectionId(aid); if (ac != null) await Clients.Client(ac).SendAsync("PrivateEnded", d.DeviceId); } } await Clients.Caller.SendAsync("PrivateEnded"); }

    public async Task DeleteMessage(string firebaseKey)
    {
        await Clients.All.SendAsync("MessageDeleted", firebaseKey);
    }
}


public class VisitorTracker
{
    private readonly ConcurrentDictionary<string, Device> _d = new();
    private readonly ConcurrentDictionary<string, string> _c2d = new();
    private readonly ConcurrentDictionary<string, string> _nick = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<PrivateMsg>>> _priv = new();
    private readonly List<ChatMsg> _msg = new();
    private readonly object _l = new();
    private readonly IHttpClientFactory _hf;
    private readonly ConcurrentDictionary<string, string> _sessions = new();

    public VisitorTracker(IHttpClientFactory hf) => _hf = hf;

    public async Task ConnectAsync(string cid, string did, string ip, string ua)
    {
        var dev = _d.GetOrAdd(did, _ => new Device { DeviceId = did, FirstIp = ip, FirstSeen = DateTime.Now, UserAgent = ua });
        dev.LastIp = ip; dev.LastSeen = DateTime.Now; dev.Online = true; dev.UserAgent = ua; _c2d[cid] = did;

        if (string.IsNullOrEmpty(dev.LocationInfo) && ip != "127.0.0.1" && ip != "0.0.0.0")
        {
            try
            {
                var c = _hf.CreateClient(); c.DefaultRequestHeaders.Add("User-Agent", "EliasConnect/1.0");
                var r = await c.GetStringAsync($"https://ip-api.com/json/{ip}?fields=country,regionName,city,lat,lon");
                var data = JsonSerializer.Deserialize<IpApiResponse>(r);
                if (data != null && !string.IsNullOrEmpty(data.country))
                {
                    if (dev.Lat == 0) { dev.Lat = data.lat; dev.Lng = data.lon; }
                    var parts = new List<string>();
                    if (!string.IsNullOrEmpty(data.country)) parts.Add(data.country);
                    if (!string.IsNullOrEmpty(data.regionName)) parts.Add(data.regionName);
                    if (!string.IsNullOrEmpty(data.city)) parts.Add(data.city);
                    dev.LocationInfo = string.Join(", ", parts);
                }
            }
            catch { }
        }
    }

    public void Disconnect(string cid) { if (_c2d.TryRemove(cid, out var did) && _d.TryGetValue(did, out var dev)) dev.Online = false; }
    public Device? GetByConnection(string cid) { _c2d.TryGetValue(cid, out var did); return did != null && _d.TryGetValue(did, out var dv) ? dv : null; }
    public string? GetOnlineConnectionId(string did) => _c2d.FirstOrDefault(x => x.Value == did).Key;
    public bool IsAdmin(string cid) => GetByConnection(cid)?.IsAdmin ?? false;
    public void SetAdmin(string cid, bool a) { var d = GetByConnection(cid); if (d != null) d.IsAdmin = a; }
    public void SetLocation(string cid, double lat, double lng, string info) { if (string.IsNullOrEmpty(info)) return; var d = GetByConnection(cid); if (d != null) { d.Lat = lat; d.Lng = lng; d.LocationInfo = info; } }

    public void AddMessage(string u, string dn, string m, bool save = true)
    {
        var msg = new ChatMsg { User = u, DeviceName = dn, Message = m, Timestamp = DateTime.Now, Save = save };
        lock (_l) { _msg.Add(msg); if (_msg.Count > 1000) _msg.RemoveAt(0); }
    }

    public void AddPrivateMessage(string aDev, string vDev, string s, string m)
    {
        var key = aDev + "|" + vDev; var list = _priv.GetOrAdd(key, _ => new ConcurrentDictionary<string, List<PrivateMsg>>());
        var msgs = list.GetOrAdd(key, _ => new List<PrivateMsg>());
        lock (msgs) { msgs.Add(new PrivateMsg { Sender = s, Message = m, Timestamp = DateTime.Now }); if (msgs.Count > 200) msgs.RemoveAt(0); }
    }

    public List<PrivateMsg> GetPrivateMessages(string aDev, string vDev)
    {
        var key = aDev + "|" + vDev;
        if (_priv.TryGetValue(key, out var list) && list.TryGetValue(key, out var msgs)) lock (msgs) return msgs.ToList();
        return new List<PrivateMsg>();
    }

    public void StartPrivateSession(string aDev, string vDev) { _sessions[aDev] = vDev; }
    public void EndPrivateSession(string aDev) { _sessions.TryRemove(aDev, out _); }
    public string? GetPrivateTarget(string aDev) { _sessions.TryGetValue(aDev, out var v); return v; }
    public string? GetPrivateAdmin(string vDev) => _sessions.FirstOrDefault(x => x.Value == vDev).Key;
    public void ClearAll() { _d.Clear(); _c2d.Clear(); }
    public void DeleteDevice(string did) { _d.TryRemove(did, out _); foreach (var kv in _c2d.Where(x => x.Value == did).ToList()) _c2d.TryRemove(kv.Key, out _); }
    public void SetNickname(string did, string n) { if (string.IsNullOrWhiteSpace(n)) _nick.TryRemove(did, out _); else _nick[did] = n; }
    public List<Device> GetAll() => _d.Values.OrderByDescending(x => x.LastSeen).Select(x => { x.Nickname = _nick.TryGetValue(x.DeviceId, out var n) ? n : ""; return x; }).ToList();
    public List<ChatMsg> GetMessages() { lock (_l) return _msg.Where(m => m.Save).ToList(); }
}

public class Device { public string DeviceId { get; set; } = ""; public string FirstIp { get; set; } = ""; public string LastIp { get; set; } = ""; public string UserAgent { get; set; } = ""; public double Lat { get; set; } public double Lng { get; set; } public string LocationInfo { get; set; } = ""; public bool Online { get; set; } public bool IsAdmin { get; set; } public DateTime FirstSeen { get; set; } public DateTime LastSeen { get; set; } public string DeviceName => DeviceId; public string Nickname { get; set; } = ""; public string DisplayName => string.IsNullOrEmpty(Nickname) ? DeviceName : Nickname; }
public class ChatMsg { public string User { get; set; } = ""; public string DeviceName { get; set; } = ""; public string Message { get; set; } = ""; public DateTime Timestamp { get; set; } public bool Save { get; set; } = true; }
public class PrivateMsg { public string Sender { get; set; } = ""; public string Message { get; set; } = ""; public DateTime Timestamp { get; set; } }
public class IpApiResponse { public string country { get; set; } = ""; public string regionName { get; set; } = ""; public string city { get; set; } = ""; public double lat { get; set; } public double lon { get; set; } }