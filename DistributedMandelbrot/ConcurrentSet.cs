using System.Collections.Generic;

namespace DistributedMandelbrot
{
    public class ConcurrentSet<T>
    {

        protected HashSet<T> items;
        protected readonly object itemLock = new();

        public ConcurrentSet()
        {
            items = new HashSet<T>();
        }

        public ConcurrentSet(IEnumerable<T> items)
        {
            this.items = new HashSet<T>(items);
        }

        public void Add(T item)
        {
            lock (itemLock)
                items.Add(item);
        }

        public void Remove(T item)
        {
            lock (itemLock)
                items.Remove(item);
        }

        public bool RemoveOneWhere(Predicate<T> predicate)
        {

            lock (itemLock)
                foreach (T item in items)
                    if (predicate.Invoke(item))
                    {
                        items.Remove(item);
                        return true;
                    }

            return false;

        }

        public int RemoveAllWhere(Predicate<T> predicate)
        {
            lock (itemLock)
                return items.RemoveWhere(predicate);
        }

        public bool Contains(T item)
        {
            lock (itemLock)
                return items.Contains(item);
        }

        public bool ContainsWhere(Predicate<T> predicate)
        {
            lock (itemLock)
                return items.Any(x => predicate(x));
        }

    }
}
