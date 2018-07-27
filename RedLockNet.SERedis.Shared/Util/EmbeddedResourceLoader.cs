using System.IO;
using System.Reflection;

namespace RedLockNet.SERedis.Util
{
	internal static class EmbeddedResourceLoader
	{
		internal static string GetEmbeddedResource(string name)
		{
#if NET40
		    var assembly = typeof(EmbeddedResourceLoader).Assembly;
#else
		    var assembly = typeof(EmbeddedResourceLoader).GetTypeInfo().Assembly;
#endif
            using (var stream = assembly.GetManifestResourceStream(name))
			using (var streamReader = new StreamReader(stream))
			{
				return streamReader.ReadToEnd();
			}
		}
	}
}