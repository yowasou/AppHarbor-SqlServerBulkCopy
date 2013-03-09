using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NDesk.Options;
using System.Data;

namespace AppHarbor.SqlServerBulkCopy
{
	class Program
	{
		static void Main(string[] args)
		{
			const double batchDataSize = 100000; //kB
			bool showHelp = false;
			string sourceServerName = null, sourceUsername = null, sourcePassword = null,
				sourceDatabaseName = null, destinationServerName = null, destinationUsername = null,
				destinationPassword = null, destinationDatabaseName = null;
			bool includeSystemTables = false, clearDestinationDatabase = false, checkIdentityExists = false;

			IEnumerable<string> ignoredTables = Enumerable.Empty<string>();
			IEnumerable<string> onlyTables = Enumerable.Empty<string>();

			var optionSet = new OptionSet() {
				{ "h|help", "show this message and exit", x => showHelp = x != null},
				{ "srcserver=", "source server (eg. db000.appharbor.net)", x => sourceServerName = x },
				{ "srcusername=", "username on source server", x => sourceUsername = x },
				{ "srcpassword=", "password on source server", x => sourcePassword = x },
				{ "srcdatabasename=", "source database name", x => sourceDatabaseName = x },
				{ "dstserver=", "destination server", x => destinationServerName = x },
				{ "dstusername=", "username on destination server", x => destinationUsername = x },
				{ "dstpassword=", "password on destination server", x => destinationPassword = x },
				{ "dstdatabasename=", "destination database name", x => destinationDatabaseName = x },
				{ "ignoretables=", "names of tables not to copy", x => ignoredTables = x.Split(',') },
				{ "onlytables=", "names of the only tables to copy", x => onlyTables = x.Split(',') },
				{ "includesystemtables", "include copying system tables (by default these are excluded). You should only use this in conjunction with 'onlytables' to opt in specific system tables", x => includeSystemTables = x != null },
				{ "cleardstdatabase", "clears the destination database before copying the data", x => clearDestinationDatabase = x != null },
				{ "checkidentityexists", "only reseed identity if table has identity column", x => checkIdentityExists = x != null }
			};

			try
			{
				optionSet.Parse(args);
				if (showHelp)
				{
					ShowHelp(optionSet);
					return;
				}
				if (sourceServerName == null)
				{
					throw new OptionException("source server not specified", "srcserver");
				}
				if (sourceUsername == null && sourcePassword != null)
				{
					throw new OptionException("source username not specified", "srcusername");
				}
				if (sourcePassword == null && sourceUsername != null)
				{
					throw new OptionException("source password not specified", "srcpassword");
				}
				if (sourceDatabaseName == null)
				{
					throw new OptionException("source database name not specified", "srcdatabasename");
				}
				if (destinationServerName == null)
				{
					throw new OptionException("destination server not specified", "dstserver");
				}
				if (destinationUsername == null && destinationPassword != null)
				{
					throw new OptionException("destination username not specified", "dstusername");
				}
				if (destinationPassword == null && destinationUsername != null)
				{
					throw new OptionException("destination password not specified", "dstpassword");
				}
				if (destinationDatabaseName == null)
				{
					throw new OptionException("destination database name not specified", "dstdatabasename");
				}
				if (onlyTables.Any() && ignoredTables.Any())
				{
					throw new OptionException("you can either opt-in for tables (--onlytables) or opt-out (--ignoretables) but not both at once.", "onlytables");
				}
			}
			catch (OptionException exception)
			{
				Console.Write("{0}: ", AppDomain.CurrentDomain.FriendlyName);
				Console.WriteLine(exception.Message);
				Console.WriteLine("Try {0} --help for more information", AppDomain.CurrentDomain.FriendlyName);
				return;
			}

			Console.WriteLine("Retrieving source database table information...");

			var usingTrustedConnection = string.IsNullOrEmpty(sourceUsername) && string.IsNullOrEmpty(sourcePassword);
			var sourceConnection = usingTrustedConnection
				? new ServerConnection(sourceServerName) { LoginSecure = true }
				: new ServerConnection(sourceServerName, sourceUsername, sourcePassword);
			var sourceServer = new Server(sourceConnection);
			var sourceDatabase = sourceServer.Databases[sourceDatabaseName];

			var tablesQuery = sourceDatabase.Tables.OfType<Table>();
			if (!includeSystemTables)
				tablesQuery = tablesQuery.Where(x => !x.IsSystemObject);
			var tables = tablesQuery
				.Select(x => new {x.Schema, x.Name, FullName = '[' + x.Schema + ']' + ".[" + x.Name + ']'})
				.ToList();

			bool isUsingOptInForTables = onlyTables.Any();
			var actualOnlyTables = tables.Where(table => onlyTables.Contains(table.FullName)).ToList();
		    var actualExcludedTables = (isUsingOptInForTables) ?
				tables.Except(actualOnlyTables)
				: tables.Where(table => ignoredTables.Contains(table.FullName)).ToList();
		    if (actualExcludedTables.Any())
			{
				Console.WriteLine("Ignoring:\n{0}", string.Join("\n", actualExcludedTables.Select(x=>x.FullName)));
			}

			tables = tables.Except(actualExcludedTables).ToList();
			Console.WriteLine("Copying {0} tables:\n{1}", tables.Count(), string.Join("\n", tables.Select(x=>x.FullName)));

			var destinationConnectionString = GetConnectionString(destinationServerName,
				destinationDatabaseName, destinationUsername, destinationPassword);

			var sourceConnectionString = GetConnectionString(sourceServerName,
				sourceDatabaseName, sourceUsername, sourcePassword);

			var watch = Stopwatch.StartNew();

			// clear the data before copying
			if (clearDestinationDatabase)
			{
				using (var connection = new SqlConnection(destinationConnectionString))
				{
					using (SqlCommand command = connection.CreateCommand())
					{
						// http://stackoverflow.com/questions/155246/how-do-you-truncate-all-tables-in-a-database-using-tsql/156813#156813
						StringBuilder commandBuilder = new StringBuilder();
						commandBuilder.Append(
							@"
							-- disable all constraints
							EXEC sp_msforeachtable ""ALTER TABLE ? NOCHECK CONSTRAINT all""

							-- delete data in all tables
							EXEC sp_msforeachtable ""DELETE FROM ?""

							-- enable all constraints
							exec sp_msforeachtable ""ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all""
						");

//                        if (checkIdentityExists)
//                        {
//                        http://stackoverflow.com/questions/6542061/reseed-sql-server-identity-columns
//                            commandBuilder.Append(
//                                @"-- reseed (auto increment to 0) on user tables with identity column
//								DECLARE @reseedSql NVARCHAR(MAX);
//								SET @reseedSql = N'';
//
//								SELECT @reseedSql = @reseedSql + N'DBCC CHECKIDENT(''' 
//									+ QUOTENAME(OBJECT_SCHEMA_NAME(col.[object_id]))
//									+ '.' + QUOTENAME(OBJECT_NAME(col.[object_id])) 
//									+ ''', RESEED, 0);' + CHAR(13) + CHAR(10)
//									FROM sys.columns as col
//									JOIN sys.tables as tbl
//									ON col.[object_id] = tbl.[object_id]
//									WHERE tbl.[type] = 'U'
//									AND col.[is_identity] = 1;
//
//								EXEC sp_executesql @reseedSql;");
//                        }
//                        else
//                        {
//                            commandBuilder.Append(@"
//								-- reseed (auto increment to 0)
//								EXEC sp_msforeachtable ""DBCC CHECKIDENT ( '?', RESEED, 0)""
//							");
//                        }

						command.CommandText = commandBuilder.ToString();

						Console.WriteLine("Clearing the destination database");
						connection.Open();
						command.ExecuteNonQuery();
					}
				}
			}
            SetConstraint(destinationConnectionString, false);
            try
            {
                foreach (var table in tables)
                {
                    using (var connection = new SqlConnection(sourceConnectionString))
                    {
                        connection.Open();
                        var columns = GetColumnNames(connection, table.Schema, table.Name);
                        Console.Write("Copying {0} ", table.FullName);
                        int rows = 0;
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = string.Format("select * from {0}", table.FullName);
                            rows = CopyDataDable(destinationConnectionString, columns, command, table.Name);
                        }
                        Console.WriteLine("Write " + rows.ToString() + " rows");
                    }
                }
            }
            finally
            {
                SetConstraint(destinationConnectionString, true);
            }
			watch.Stop();
			Console.WriteLine("Copy complete, total time {0} s", watch.ElapsedMilliseconds/1000);
		}

        /// <summary>
        /// 全制約の有効化または無効化を行う
        /// </summary>
        /// <param name="destinationConnectionString"></param>
        /// <param name="bCheck"></param>
        private static void SetConstraint(string destinationConnectionString, bool bCheck)
        {
            var connectionDst = new SqlConnection(destinationConnectionString);
            connectionDst.Open();
            try
            {                							
                var commandDst = connectionDst.CreateCommand();
                if (bCheck)
                {
                    commandDst.CommandText = "EXEC sp_msforeachtable \"ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all\"";
                }
                else
                {
                    commandDst.CommandText = "EXEC sp_msforeachtable \"ALTER TABLE ? NOCHECK CONSTRAINT all\"";
                }
                commandDst.ExecuteScalar();
            }
            finally
            {
                connectionDst.Close();
                connectionDst.Dispose();
            }
        }
        private static int CopyDataDable(string destinationConnectionString, List<string> columns, SqlCommand command, string tableName)
        {
            int updateCount = 0;
            SqlDataAdapter sda = new SqlDataAdapter(command);
            DataTable dt = new DataTable(tableName);
            sda.FillSchema(dt, SchemaType.Source);
            sda.Fill(dt);
            var connectionDst = new SqlConnection(destinationConnectionString);
            connectionDst.Open();
            try
            {
                var commandDst = connectionDst.CreateCommand();
                commandDst.CommandText = string.Format("select * from {0}", tableName);
                SqlDataAdapter sdaDst = new SqlDataAdapter(commandDst);
                SqlCommandBuilder builder = new SqlCommandBuilder(sdaDst);
                DataTable dtDst = new DataTable(tableName);
                sdaDst.Fill(dtDst);
                foreach (DataRow dr in dt.Rows)
                {
                    DataRow drNew = dtDst.NewRow();
                    foreach (var c in columns)
                    {
                        drNew[c] = dr[c];
                    }
                    dtDst.Rows.Add(drNew);
                    updateCount++;
                }
                sdaDst.Update(dtDst);
            }
            finally
            {
                connectionDst.Close();
                connectionDst.Dispose();
            }
            return updateCount;
        }

		private static List<string> GetColumnNames(SqlConnection connection, string schemaName, string tableName)
		{
			var sql =
				@"select column_name
				from information_schema.columns 
				where table_name = @tablename
                and table_schema = @schemaName
				and columnproperty(object_id(@tablename),column_name,'iscomputed') != 1";

			using (var command = connection.CreateCommand()) {
				command.CommandText = sql;
				command.Parameters.Add(new SqlParameter("@tablename", tableName));
                command.Parameters.Add(new SqlParameter("@schemaName", schemaName));

				var cnames = new List<string>();
				using (var reader = command.ExecuteReader()) {
					while (reader.Read()) {
						cnames.Add((string)reader[0]);
					}
				}

				return cnames;
			}
		}

		private static void SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			Console.Write(".");
		}

		private static void ShowHelp(OptionSet optionSet)
		{
			Console.WriteLine("Copy data between Microsoft SQL Server databases");
			Console.WriteLine();
			Console.WriteLine("Options:");
			optionSet.WriteOptionDescriptions(Console.Out);
		}

		static string GetConnectionString(string serverName, string databaseName, string username, string password)
		{
			var usingTrustedConnection =
				string.IsNullOrEmpty(username) &&
				string.IsNullOrEmpty(password);

			var connectionStringFormat = usingTrustedConnection
				? "Server={0};Database={1};Trusted_Connection=True;"
				: "Server={0};Database={1};User ID={2};Password={3};";

			return string.Format(connectionStringFormat, serverName, databaseName, username, password);
		}
	}
}
