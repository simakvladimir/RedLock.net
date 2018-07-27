using System;
using System.Collections.Generic;

namespace RedLockNet.SERedis.Configuration
{
	public class RedLockConfiguration
	{
		public RedLockConfiguration(IList<RedLockEndPoint> endPoints)
		{
			this.ConnectionProvider = new InternallyManagedRedLockConnectionProvider()
			{
				EndPoints = endPoints
			};
		}

		public RedLockConfiguration(RedLockConnectionProvider connectionProvider)
		{
			this.ConnectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider), "Connection provider must not be null");
		}

		public RedLockConnectionProvider ConnectionProvider { get; }
	}
}