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

using System.Collections.Concurrent;

namespace GenOnlineService
{
	/// <summary>
	/// One match being streamed for live spectating. The lobby owner's client appends
	/// the bytes of its replay recording as the match runs; observers read them back
	/// behind a fixed broadcast delay, so a spectator can never feed a player anything
	/// the player could not already see themselves a minute earlier.
	/// </summary>
	public class Livestream
	{
		public const int BroadcastDelaySeconds = 60;
		public const int MaxBytes = 32 * 1024 * 1024;

		public Int64 LobbyID { get; private set; }
		public Int64 StreamerUserID { get; private set; }
		public string Name { get; private set; }
		public string MapName { get; private set; }
		public string MapPath { get; private set; }
		public int Players { get; private set; }
		public DateTime TimeStarted { get; private set; } = DateTime.UtcNow;
		public DateTime TimeLastActivity { get; private set; } = DateTime.UtcNow;
		public bool Ended { get; private set; } = false;

		private readonly object m_lock = new();
		private readonly MemoryStream m_bytes = new();
		// End offset of each chunk and when it arrived; the broadcast delay is enforced
		// per chunk so a burst of frames does not leak early.
		private readonly List<(long endOffset, DateTime arrived)> m_chunks = new();

		public Livestream(Int64 lobbyID, Int64 streamerUserID, string name, string mapName, string mapPath, int players)
		{
			LobbyID = lobbyID;
			StreamerUserID = streamerUserID;
			Name = name;
			MapName = mapName;
			MapPath = mapPath;
			Players = players;
		}

		public long Length
		{
			get { lock (m_lock) { return m_bytes.Length; } }
		}

		/// <summary>
		/// Appends a chunk that must start exactly at the current end of the stream.
		/// Returns false and the real length when the offsets disagree, so the streamer
		/// can resend from the right place after a lost or duplicated request.
		/// </summary>
		public bool Append(long offset, byte[] data, out long currentLength)
		{
			lock (m_lock)
			{
				currentLength = m_bytes.Length;
				if (Ended || offset != m_bytes.Length || m_bytes.Length + data.Length > MaxBytes)
				{
					return false;
				}
				m_bytes.Seek(0, SeekOrigin.End);
				m_bytes.Write(data, 0, data.Length);
				m_chunks.Add((m_bytes.Length, DateTime.UtcNow));
				currentLength = m_bytes.Length;
				TimeLastActivity = DateTime.UtcNow;
				return true;
			}
		}

		public void End()
		{
			lock (m_lock)
			{
				Ended = true;
				TimeLastActivity = DateTime.UtcNow;
			}
		}

		/// <summary>Bytes an observer may read right now: everything once the stream has ended, otherwise only chunks older than the broadcast delay.</summary>
		public long AvailableLength()
		{
			lock (m_lock)
			{
				if (Ended)
				{
					return m_bytes.Length;
				}
				DateTime cutoff = DateTime.UtcNow.AddSeconds(-BroadcastDelaySeconds);
				long available = 0;
				foreach (var chunk in m_chunks)
				{
					if (chunk.arrived > cutoff)
					{
						break;
					}
					available = chunk.endOffset;
				}
				return available;
			}
		}

		public byte[] Read(long from, int maxBytes)
		{
			lock (m_lock)
			{
				long available = AvailableLength();
				if (from < 0 || from >= available)
				{
					return Array.Empty<byte>();
				}
				int count = (int)Math.Min(maxBytes, available - from);
				byte[] result = new byte[count];
				m_bytes.Seek(from, SeekOrigin.Begin);
				int read = m_bytes.Read(result, 0, count);
				if (read < count)
				{
					Array.Resize(ref result, read);
				}
				return result;
			}
		}
	}

	/// <summary>
	/// In-memory store of live match streams, keyed by lobby. Streams live as long as
	/// the match plus a grace period for observers to drain the tail; a streamer that
	/// goes quiet (crash, disconnect) has its stream ended for it.
	/// </summary>
	public class LivestreamManager
	{
		public const int StreamerSilenceTimeoutSeconds = 120;
		public const int RetainAfterEndSeconds = 10 * 60;

		private readonly ConcurrentDictionary<Int64, Livestream> m_streams = new();

		public Livestream? Get(Int64 lobbyID)
		{
			Cleanup();
			m_streams.TryGetValue(lobbyID, out Livestream? stream);
			return stream;
		}

		public Livestream GetOrCreate(Int64 lobbyID, Int64 streamerUserID, string name, string mapName, string mapPath, int players)
		{
			Cleanup();
			return m_streams.GetOrAdd(lobbyID, id => new Livestream(id, streamerUserID, name, mapName, mapPath, players));
		}

		public void Remove(Int64 lobbyID)
		{
			m_streams.TryRemove(lobbyID, out _);
		}

		public List<Livestream> GetLive()
		{
			Cleanup();
			List<Livestream> result = new();
			foreach (Livestream stream in m_streams.Values)
			{
				if (!stream.Ended)
				{
					result.Add(stream);
				}
			}
			return result;
		}

		private void Cleanup()
		{
			DateTime now = DateTime.UtcNow;
			foreach (var kvp in m_streams)
			{
				Livestream stream = kvp.Value;
				if (!stream.Ended && (now - stream.TimeLastActivity).TotalSeconds > StreamerSilenceTimeoutSeconds)
				{
					stream.End();
				}
				else if (stream.Ended && (now - stream.TimeLastActivity).TotalSeconds > RetainAfterEndSeconds)
				{
					m_streams.TryRemove(kvp.Key, out _);
				}
			}
		}
	}
}
