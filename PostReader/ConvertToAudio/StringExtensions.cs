using System;
using System.Collections.Generic;

namespace ConvertToAudio
{
    public static class StringExtensions
    {
        public static IEnumerable<string> Chunk(this string str, int estimateChunckSize)
        {
			var maxChunkSize = estimateChunckSize + 100;
            var copyStr = string.Copy(str);
            var chunks = new List<string>();
			while (copyStr.Length > maxChunkSize)
            {
                var begin = 0;
				var end = copyStr.IndexOf(".", estimateChunckSize, StringComparison.Ordinal) + 1;
                if (end == -1)
                {
					end = copyStr.IndexOf(" ", estimateChunckSize, StringComparison.Ordinal) + 1;
                }
                var chunk = copyStr.Substring(begin, end);
                copyStr = copyStr.Substring(end, copyStr.Length - end);
                chunks.Add(chunk);
            }
            chunks.Add(copyStr);
            return chunks;
        }
    }
}
