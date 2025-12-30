using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SqliteStreamer
{
    public class ConsumablePushEnumerable<T> : IDisposable
    {
        private readonly static Exception Completed = new Exception("Completed.");
        private readonly static Exception Disposed = new Exception("Disposed.");

        private readonly int batchSize;

        private bool enumerableTaken = false;
        private int pollingTimeMs;


        public long Count { get; private set; }

        public ConsumablePushEnumerable(int batchSize = 1000, int pollingTimeMs = 100)
        {
            this.batchSize = batchSize;
            this.pollingTimeMs = pollingTimeMs;
            this.currentBatch = new List<T>(batchSize);
        }


        record struct Message(List<T>? Batch, Exception? Exception);

        private List<T> currentBatch;
        private readonly ConcurrentQueue<Message> queue = new();

        public void ConsumeAsync(Action<IEnumerable<T>> process, out Action markCompletedAndWaitWriteEnd)
        {
            var task = Task.Run(() =>
            {
                process(GetConsumingEnumerable());
            });
            markCompletedAndWaitWriteEnd = () =>
            {
                this.SetCompleted();
                task.GetAwaiter().GetResult();
            };

        }

        public void AddRange(IEnumerable<T> items)
        {
            foreach (var item in items)
            {
                Add(item);
            }
        }


        public void Add(T item)
        {
            Count++;
            currentBatch.Add(item);
            if (currentBatch.Count == batchSize)
            {
                FlushBatch();
            }
        }

        private void FlushBatch()
        {
            queue.Enqueue(new Message(currentBatch, null));
            currentBatch = new List<T>(batchSize);
        }

        public void SetCompleted()
        {
            FlushBatch();
            queue.Enqueue(new Message(null, Completed));
        }
        public void Dispose()
        {
            queue.Enqueue(new Message(null, Disposed));
        }
        public void SetException(Exception exception)
        {
            queue.Enqueue(new Message(null, exception));
        }


        public IEnumerable<T> GetConsumingEnumerable()
        {
            if (enumerableTaken) throw new InvalidOperationException("GetConsumingEnumerable already called.");
            enumerableTaken = true;
            while (true)
            {
                if (!queue.TryDequeue(out var msg))
                {
                    Thread.Sleep(pollingTimeMs);
                    continue;
                }

                if (msg.Exception != null)
                {
                    if (msg.Exception == Disposed)
                    {
                        throw new ObjectDisposedException(nameof(ConsumablePushEnumerable<T>));
                    }
                    else if (msg.Exception == Completed) break;
                    else throw msg.Exception;
                }

                foreach (var item in msg.Batch!)
                {
                    yield return item;
                }
            }
        }


    }
}
