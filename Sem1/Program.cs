using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace DualModeSemaphoreDemo
{
	public enum Mode
	{
		None = 0,
		A,
		B
	}

	public sealed class ChannelDualModeSemaphore
	{
		private readonly object _lock = new();

		private Mode _current = Mode.None;
		private int _count = 0;

		// actual wait queue
		private readonly Channel<Waiter> _queue =
			Channel.CreateUnbounded<Waiter>();

		// debug mirror queue (for printing only)
		private readonly List<Mode> _debugQueue = new();

		private sealed class Waiter
		{
			public Mode Mode;
			public TaskCompletionSource<bool> Tcs =
				new(TaskCreationOptions.RunContinuationsAsynchronously);
		}

		public async ValueTask EnterAsync(Mode requested)
		{
			lock (_lock)
			{
				if (_current == Mode.None || _current == requested)
				{
					_current = requested;
					_count++;
					PrintState($"ENTER FAST {requested}");
					return;
				}
			}

			var waiter = new Waiter { Mode = requested };

			lock (_lock)
			{
				_debugQueue.Add(requested);
				PrintState($"QUEUE {requested}");
			}

			await _queue.Writer.WriteAsync(waiter);
			await waiter.Tcs.Task;
		}

		public ValueTask ExitAsync()
		{
			bool drain = false;

			lock (_lock)
			{
				_count--;

				if (_count == 0)
				{
					_current = Mode.None;
					drain = true;
					PrintState("STATE -> NONE");
				}
				else
				{
					PrintState("EXIT");
				}
			}

			if (drain)
				DrainAllSameMode();

			return ValueTask.CompletedTask;
		}

		private void DrainAllSameMode()
		{
			var temp = new List<Waiter>();

			while (_queue.Reader.TryRead(out var w))
				temp.Add(w);

			if (temp.Count == 0)
			{
				lock (_lock)
					PrintState("QUEUE EMPTY");
				return;
			}

			var mode = temp[0].Mode;

			foreach (var waiter in temp)
			{
				if (waiter.Mode == mode)
				{
					lock (_lock)
					{
						_current = mode;
						_count++;

						// remove one from debug queue
						var idx = _debugQueue.IndexOf(mode);
						if (idx >= 0)
							_debugQueue.RemoveAt(idx);

						PrintState($"DRAIN {mode}");
					}

					waiter.Tcs.SetResult(true);
				}
				else
				{
					_queue.Writer.TryWrite(waiter);
				}
			}
		}

		private void PrintState(string action)
		{
			var queue = _debugQueue.Count == 0
				? "-"
				: string.Join(" ", _debugQueue.Select(x => x.ToString()));

			Console.WriteLine(
				$"{DateTime.Now:HH:mm:ss.fff} | " +
				$"{action,-12} | " +
				$"STATE={_current} " +
				$"COUNT={_count} " +
				$"QUEUE=[ {queue} ]");
		}
	}

	class Program
	{
		static async Task Worker(
			int id,
			Mode mode,
			ChannelDualModeSemaphore sem,
			Random rnd)
		{
			while (true)
			{
				await Task.Delay(rnd.Next(400, 1200));

				Console.WriteLine(
					$"{DateTime.Now:HH:mm:ss.fff} T{id} REQUEST {mode}");

				await sem.EnterAsync(mode);

				Console.WriteLine(
					$"{DateTime.Now:HH:mm:ss.fff} >>> T{id} ENTER {mode}");

				await Task.Delay(rnd.Next(700, 1500));

				await sem.ExitAsync();

				Console.WriteLine(
					$"{DateTime.Now:HH:mm:ss.fff} <<< T{id} EXIT  {mode}");
			}
		}

		static async Task Main()
		{
			var sem = new ChannelDualModeSemaphore();

			var tasks = new List<Task>();

			// simulate small number of threads
			tasks.Add(Task.Run(() => Worker(1, Mode.A, sem, new Random(1))));
			tasks.Add(Task.Run(() => Worker(2, Mode.A, sem, new Random(2))));
			tasks.Add(Task.Run(() => Worker(3, Mode.B, sem, new Random(3))));
			tasks.Add(Task.Run(() => Worker(4, Mode.B, sem, new Random(4))));
			tasks.Add(Task.Run(() => Worker(5, Mode.A, sem, new Random(5))));
			tasks.Add(Task.Run(() => Worker(6, Mode.A, sem, new Random(6))));

			Console.WriteLine("Running... Ctrl+C to stop\n");

			await Task.WhenAll(tasks);
		}
	}
}