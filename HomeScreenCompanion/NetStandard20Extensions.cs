using System.Collections.Generic;

namespace HomeScreenCompanion
{
    internal static class NetStandard20Extensions
    {
        public static bool TryAdd<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key, TValue value)
        {
            if (dict.ContainsKey(key)) return false;
            dict[key] = value;
            return true;
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key)
        {
            dict.TryGetValue(key, out var value);
            return value;
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key, TValue defaultValue)
        {
            return dict.TryGetValue(key, out var value) ? value : defaultValue;
        }

        public static System.Collections.Generic.HashSet<T> ToHashSet<T>(this System.Collections.Generic.IEnumerable<T> source)
        {
            return new System.Collections.Generic.HashSet<T>(source);
        }

        public static System.Collections.Generic.HashSet<T> ToHashSet<T>(this System.Collections.Generic.IEnumerable<T> source, System.Collections.Generic.IEqualityComparer<T> comparer)
        {
            return new System.Collections.Generic.HashSet<T>(source, comparer);
        }
    }
}
