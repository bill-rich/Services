/*
**    GeneralsOnline Game Services - Backend Services for Command & Conquer Generals Online: Zero Hour
**    Copyright (C) 2025  GeneralsOnline Development Team
**
**    This program is free software: you can redistribute it and/or modify
**    it under the terms of the GNU Affero General Public License as
**    published by the Free Software Foundation, either version 3 of the
**    License, or (at your option) any later version.
**
**    This program is distributed in the hope that it will be useful,
**    but WITHOUT ANY WARRANTY; without even the implied warranty of
**    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
**    GNU Affero General Public License for more details.
**
**    You should have received a copy of the GNU Affero General Public License
**    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using System.Net;
using System.Text.Json;

namespace GenOnlineService.Controllers
{
	public class LivestreamListEntry
	{
		public Int64 lobby_id { get; set; }
		public string name { get; set; } = String.Empty;
		public string map_name { get; set; } = String.Empty;
		public string map_path { get; set; } = String.Empty;
		public bool map_official { get; set; }
		public int players { get; set; }
		public long total_bytes { get; set; }
		public int seconds_live { get; set; }
	}

	public class RouteHandler_GET_Livestreams_Result : APIResult
	{
		public override Type GetReturnType() { return this.GetType(); }
		public List<LivestreamListEntry> streams { get; set; } = new();
	}

	public class RouteHandler_GET_Livestream_Result : APIResult
	{
		public override Type GetReturnType() { return this.GetType(); }
		public bool success { get; set; } = false;
		public long total { get; set; } = 0;
		public long available { get; set; } = 0;
		public bool ended { get; set; } = false;
		public long from { get; set; } = 0;
		public string data { get; set; } = String.Empty;
	}

	public class RouteHandler_POST_Livestream_Result : APIResult
	{
		public override Type GetReturnType() { return this.GetType(); }
		public bool success { get; set; } = false;
		public long total { get; set; } = 0;
	}

	/// <summary>
	/// Live spectating of matches in progress. The lobby owner's client POSTs the bytes
	/// of its replay recording as chunks; observers list the streams and GET the bytes
	/// back behind the broadcast delay, appending them to a local replay file that the
	/// game plays as it grows. Companion to the GameClient live-observer change.
	/// </summary>
	[ApiController]
	[DisableRateLimiting] // the streamer posts once a second for the whole match; the built-in limiter budget is far smaller
	[Route("env/{environment}/contract/{contract_version}/[controller]")]
	public class LivestreamController : ControllerBase
	{
		private const int MaxChunkBytes = 512 * 1024;
		private const int MaxReadBytes = 256 * 1024;
		private const int MaxChunkRequestBytes = MaxChunkBytes * 4 / 3 + 4096; // base64 plus JSON framing

		private readonly LivestreamManager _livestreams;
		private readonly LobbyManager _lobbyManager;
		private readonly ILogger<LivestreamController> _logger;

		public LivestreamController(LivestreamManager livestreams, LobbyManager lobbyManager, ILogger<LivestreamController> logger)
		{
			_livestreams = livestreams;
			_lobbyManager = lobbyManager;
			_logger = logger;
		}

		[HttpGet]
		[Authorize(Roles = "GameClient,ChatClient,GameLauncher,Monitor")]
		public APIResult GetList()
		{
			RouteHandler_GET_Livestreams_Result result = new();
			DateTime now = DateTime.UtcNow;
			foreach (Livestream stream in _livestreams.GetLive())
			{
				result.streams.Add(new LivestreamListEntry
				{
					lobby_id = stream.LobbyID,
					name = stream.Name,
					map_name = stream.MapName,
					map_path = stream.MapPath,
					map_official = stream.MapOfficial,
					players = stream.Players,
					total_bytes = stream.Length,
					seconds_live = (int)(now - stream.TimeStarted).TotalSeconds
				});
			}
			return result;
		}

		[HttpGet("{lobbyID}")]
		[Authorize(Roles = "GameClient,ChatClient,GameLauncher,Monitor")]
		public APIResult Get(Int64 lobbyID, [FromQuery] long from = 0)
		{
			RouteHandler_GET_Livestream_Result result = new();
			Livestream? stream = _livestreams.Get(lobbyID);
			if (stream == null)
			{
				Response.StatusCode = (int)HttpStatusCode.NotFound;
				return result;
			}

			// A player in the match must not be able to watch it from a second client and
			// see the whole map a delay behind; the stream opens to them once it has ended.
			if (!stream.Ended)
			{
				Int64 user_id = TokenHelper.GetUserID(this);
				Lobby? lobby = _lobbyManager.GetLobby(lobbyID);
				if (lobby != null && user_id != -1 && lobby.GetMemberFromUserID(user_id) != null)
				{
					Response.StatusCode = (int)HttpStatusCode.Forbidden;
					return result;
				}
			}

			result.success = true;
			result.total = stream.Length;
			result.available = stream.AvailableLength();
			result.ended = stream.Ended;
			result.from = from;
			byte[] bytes = stream.Read(from, MaxReadBytes);
			if (bytes.Length > 0)
			{
				result.data = Convert.ToBase64String(bytes);
			}
			return result;
		}

		[HttpPost("{lobbyID}/chunk")]
		[Authorize(Roles = "GameClient")]
		[RequestSizeLimit(MaxChunkRequestBytes)]
		public async Task<APIResult> PostChunk(Int64 lobbyID)
		{
			RouteHandler_POST_Livestream_Result result = new();

			Int64 user_id = TokenHelper.GetUserID(this);
			if (user_id == -1)
			{
				Response.StatusCode = (int)HttpStatusCode.Unauthorized;
				return result;
			}

			Livestream? existing = _livestreams.Get(lobbyID);
			if (existing == null)
			{
				// Only the lobby owner starts a stream, only while the match is running, and
				// only for a match that admits observers and is not passworded: a private
				// game is not broadcast. Every member records the same replay, so one copy
				// is enough.
				Lobby? lobby = _lobbyManager.GetLobby(lobbyID);
				if (lobby == null || lobby.Owner != user_id || lobby.State != ELobbyState.INGAME
					|| !lobby.AllowObservers || lobby.IsPassworded)
				{
					Response.StatusCode = (int)HttpStatusCode.Unauthorized;
					return result;
				}
			}
			else if (existing.StreamerUserID != user_id)
			{
				// Once a stream exists its streamer may keep appending even after the lobby
				// leaves INGAME, so the final chunks are not lost when the lobby completes first.
				Response.StatusCode = (int)HttpStatusCode.Conflict;
				result.total = existing.Length;
				return result;
			}

			long offset;
			byte[] bytes;
			try
			{
				using var reader = new StreamReader(HttpContext.Request.Body);
				string jsonData = await reader.ReadToEndAsync();
				var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(jsonData, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
				if (data == null || !data.ContainsKey("offset") || !data.ContainsKey("data"))
				{
					Response.StatusCode = (int)HttpStatusCode.BadRequest;
					return result;
				}
				offset = data["offset"].GetInt64();
				bytes = Convert.FromBase64String(data["data"].GetString() ?? String.Empty);
			}
			catch
			{
				Response.StatusCode = (int)HttpStatusCode.BadRequest;
				return result;
			}

			if (bytes.Length == 0 || bytes.Length > MaxChunkBytes)
			{
				Response.StatusCode = (int)HttpStatusCode.BadRequest;
				return result;
			}

			Livestream stream;
			if (existing != null)
			{
				stream = existing;
			}
			else
			{
				Lobby lobby = _lobbyManager.GetLobby(lobbyID)!;
				stream = _livestreams.GetOrCreate(lobbyID, user_id, lobby.Name, lobby.MapName, lobby.MapPath, lobby.IsMapOfficial, lobby.GetNumberOfHumans());
				if (stream.StreamerUserID != user_id)
				{
					Response.StatusCode = (int)HttpStatusCode.Conflict;
					result.total = stream.Length;
					return result;
				}
			}

			if (!stream.Append(offset, bytes, out long currentLength))
			{
				// Offset disagreement (lost reply, duplicate send) or the stream is over:
				// tell the streamer where we really are so it can resume from there.
				Response.StatusCode = (int)HttpStatusCode.Conflict;
				result.total = currentLength;
				return result;
			}

			result.success = true;
			result.total = currentLength;
			return result;
		}

		[HttpPost("{lobbyID}/end")]
		[Authorize(Roles = "GameClient")]
		public APIResult PostEnd(Int64 lobbyID)
		{
			RouteHandler_POST_Livestream_Result result = new();
			Int64 user_id = TokenHelper.GetUserID(this);
			Livestream? stream = _livestreams.Get(lobbyID);
			if (stream == null)
			{
				Response.StatusCode = (int)HttpStatusCode.NotFound;
				return result;
			}
			if (user_id == -1 || stream.StreamerUserID != user_id)
			{
				Response.StatusCode = (int)HttpStatusCode.Unauthorized;
				return result;
			}
			stream.End();
			result.success = true;
			result.total = stream.Length;
			return result;
		}
	}
}
