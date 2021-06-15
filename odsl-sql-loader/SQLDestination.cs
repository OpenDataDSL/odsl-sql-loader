using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using System;
using System.Data;

namespace odsl_sql_loader
{
    public class SQLDestination
    {
        IConfiguration configuration;
        public SQLDestination(IConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void WriteMessage(String message)
        {
            using (SqlConnection connection = new SqlConnection(configuration["connectionStrings:sqlserver"]))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "dbo.InsertOrUpdateFXRates";
                    command.CommandType = CommandType.StoredProcedure;
                    SqlParameter parameter = command.Parameters
                                      .AddWithValue("@Rates", CreateDataTable(message));
                    parameter.SqlDbType = SqlDbType.Structured;
                    parameter.TypeName = "dbo.FXRates";

                    command.ExecuteNonQuery();
                }
            }
        }

        public DataTable CreateDataTable(String message)
        {
            DataTable table = new DataTable();
            table.Columns.Add("base", typeof(String));
            table.Columns.Add("currency", typeof(String));
            table.Columns.Add("index", typeof(DateTime));
            table.Columns.Add("value", typeof(Decimal));

            dynamic data = JObject.Parse(message);
            foreach (dynamic ccy in data.status)
            {
                string fxbase = "EUR";
                string fxccy = ccy.Name;
                foreach (dynamic values in ccy.Value.tenors)
                {
                    DateTime fxindex = values.time;
                    Decimal fxvalue = values.value;
                    table.Rows.Add(fxbase, fxccy, fxindex, fxvalue);
                }
            }
            return table;
        }
    }
}
