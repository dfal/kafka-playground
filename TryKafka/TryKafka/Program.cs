using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace TryKafka
{
	class Program
	{
		static void Main(string[] args)
		{
			const string brokerList = "192.168.50.5";
			const string topicName = "test";

			var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
			using (var producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
			{
				var cancelled = false;
				Console.CancelKeyPress += (_, e) =>
				{
					e.Cancel = true; // prevent the process from terminating.
					cancelled = true;
				};

				while (!cancelled)
				{
					Console.Write("> ");

					string text;
					try
					{
						text = Console.ReadLine();
					}
					catch (IOException)
					{
						// IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
						break;
					}
					if (text == null)
					{
						// Console returned null before 
						// the CancelKeyPress was treated
						break;
					}

					var key = "";
					var val = text;

					// split line if both key and value specified.
					int index = text.IndexOf(" ");
					if (index != -1)
					{
						key = text.Substring(0, index);
						val = text.Substring(index + 1);
					}

					var deliveryReport = producer.ProduceAsync(topicName, key, val, PartitioningFunc(key));
					var result = deliveryReport.Result; // synchronously waits for message to be produced.
					Console.WriteLine($"Partition: {result.Partition}, Offset: {result.Offset}");
				}
			}
			
		}
		static int PartitioningFunc(string key)
		{
			return (int) key[0];
		}
	}
}
