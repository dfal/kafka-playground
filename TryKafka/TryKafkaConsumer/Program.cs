using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace TryKafkaConsumer
{
	public class Program
	{
		private static Dictionary<string, object> constructConfig(string brokerList, bool enableAutoCommit) =>
			new Dictionary<string, object>
			{
				{ "group.id", "advanced-csharp-consumer" },
				{ "enable.auto.commit", enableAutoCommit },
				{ "auto.commit.interval.ms", 5000 },
				{ "statistics.interval.ms", 60000 },
				{ "bootstrap.servers", brokerList },
				{ "default.topic.config", new Dictionary<string, object>()
					{
						{ "auto.offset.reset", "smallest" }
					}
				}
			};

		/// <summary>
		//      In this example:
		///         - offsets are auto commited.
		///         - consumer.Poll / OnMessage is used to consume messages.
		///         - no extra thread is created for the Poll loop.
		/// </summary>
		public static void Run_Poll(string brokerList, List<string> topics)
		{
			using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, true), null, new StringDeserializer(Encoding.UTF8)))
			{
				// Note: All event handlers are called on the main thread.

				consumer.OnMessage += (_, msg)
					=> Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

				consumer.OnPartitionEOF += (_, end)
					=> Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

				consumer.OnError += (_, error)
					=> Console.WriteLine($"Error: {error}");

				consumer.OnConsumeError += (_, msg)
					=> Console.WriteLine($"Error consuming from topic/partition/offset {msg.Topic}/{msg.Partition}/{msg.Offset}: {msg.Error}");

				consumer.OnOffsetsCommitted += (_, commit) =>
				{
					Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

					if (commit.Error)
					{
						Console.WriteLine($"Failed to commit offsets: {commit.Error}");
					}
					Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
				};

				consumer.OnPartitionsAssigned += (_, partitions) =>
				{
					Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
					consumer.Assign(partitions);
				};

				consumer.OnPartitionsRevoked += (_, partitions) =>
				{
					Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
					consumer.Unassign();
				};

				consumer.OnStatistics += (_, json)
					=> Console.WriteLine($"Statistics: {json}");

				consumer.Subscribe(topics);

				Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

				var cancelled = false;
				Console.CancelKeyPress += (_, e) =>
				{
					e.Cancel = true; // prevent the process from terminating.
					cancelled = true;
				};

				Console.WriteLine("Ctrl-C to exit.");
				while (!cancelled)
				{
					consumer.Poll(TimeSpan.FromMilliseconds(100));
				}
			}
		}

		/// <summary>
		///     In this example
		///         - offsets are manually committed.
		///         - consumer.Consume is used to consume messages.
		///             (all other events are still handled by event handlers)
		///         - no extra thread is created for the Poll (Consume) loop.
		/// </summary>
		public static void Run_Consume(string brokerList, List<string> topics)
		{
			using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, false), null, new StringDeserializer(Encoding.UTF8)))
			{
				// Note: All event handlers are called on the main thread.

				consumer.OnPartitionEOF += (_, end)
					=> Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

				consumer.OnError += (_, error)
					=> Console.WriteLine($"Error: {error}");

				consumer.OnPartitionsAssigned += (_, partitions) =>
				{
					Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
					consumer.Assign(partitions);
				};

				consumer.OnPartitionsRevoked += (_, partitions) =>
				{
					Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
					consumer.Unassign();
				};

				consumer.OnStatistics += (_, json)
					=> Console.WriteLine($"Statistics: {json}");

				consumer.Subscribe(topics);

				Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

				var cancelled = false;
				Console.CancelKeyPress += (_, e) =>
				{
					e.Cancel = true; // prevent the process from terminating.
					cancelled = true;
				};

				while (!cancelled)
				{
					Message<Null, string> msg;
					if (!consumer.Consume(out msg, TimeSpan.FromMilliseconds(100)))
					{
						continue;
					}

					Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

					if (msg.Offset % 5 == 0)
					{
						Console.WriteLine($"Committing offset");
						var committedOffsets = consumer.CommitAsync(msg).Result;
						Console.WriteLine($"Committed offset: {committedOffsets}");
					}
				}
			}
		}

		private static void PrintUsage()
			=> Console.WriteLine("usage: <poll|consume> <broker,broker,..> <topic> [topic..]");

		public static void Main(string[] args)
		{
			/*if (args.Length < 3)
			{
				PrintUsage();
				return;
			}

			var mode = args[0];
			var brokerList = args[1];
			var topics = args.Skip(2).ToList();

			switch (mode)
			{
				case "poll":
					Run_Poll(brokerList, topics);
					break;
				case "consume":
					Run_Consume(brokerList, topics);
					break;
				default:
					PrintUsage();
					break;
			}*/

			const string brokerList = "192.168.50.5";
			List<string> topics = new List<string> {"test"};

			Run_Consume(brokerList, topics);
		}
	}
}
