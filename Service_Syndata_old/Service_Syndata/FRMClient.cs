using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Xml;
using System.Data;
using Npgsql;
using System.ServiceModel.Description;
using System.Text;

namespace Service_Syndata
{
    class FRMClient
    {
        public static OrganizationServiceProxy crmServices;

        public static string clm { get; private set; }

        public void Syndata()
        {
            string connectString = ConfigurationManager.AppSettings.Get("CS");
            int interval = int.Parse(ConfigurationManager.AppSettings.Get("Interval"));
            string configfile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Config.xml");
            string keyfile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Synkey.xml");

            string url = ConfigurationManager.AppSettings.Get("CRMUrl");
            string domain = ConfigurationManager.AppSettings.Get("Domain");
            string user = ConfigurationManager.AppSettings.Get("User");
            string pass = ConfigurationManager.AppSettings.Get("Pass");

            ClientCredentials clientCredentials = new ClientCredentials();
            clientCredentials.UserName.UserName = domain + "\\" + user;
            clientCredentials.UserName.Password = pass;

            XmlDocument xdoc = new XmlDocument();
            xdoc.Load(configfile);

            do
            {
                try
                {
                    crmServices = new OrganizationServiceProxy(new Uri(url), null, clientCredentials, null);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            } while (crmServices == null);
            Console.WriteLine("Ket noi thanh cong");
            crmServices.EnableProxyTypes();
            Console.WriteLine("Start");
            while (true)
            {
                Console.WriteLine("Restart");
                try
                {
                    #region Start try
                    DateTime currentTime = DateTime.Now;

                    XmlDocument xkey = new XmlDocument();
                    xkey.Load(keyfile);
                    foreach (XmlNode a in xdoc.GetElementsByTagName("Table"))
                    {
                        string key = "";
                        string type = a.Attributes["type"].Value;
                        string from = a.Attributes["from"].Value;
                        string to = a.Attributes["to"].Value;
                        string synkey = a.Attributes["synkey"].Value;
                        //DateTime lastTime = DateTime.Parse(ConfigurationManager.AppSettings.Get(synkey));
                        XmlNodeList xnList = xkey.SelectNodes("/Keys/Key[@value='" + synkey + "']");
                        XmlNode xn = xnList[0];
                        DateTime lastTime = DateTime.Parse(xn.InnerText);

                        if (type == "0") //CRM to Client
                        {
                            lastTime = lastTime.AddMinutes(-15);
                            Console.WriteLine("Syn from CRM - Client [ " + from + "] : ");

                            #region type 0
                            Dictionary<Guid, int> listInsert = new Dictionary<Guid, int>();
                            NpgsqlConnection conn = new NpgsqlConnection(connectString);
                            conn.Open();

                            NpgsqlTransaction myTrans = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                            NpgsqlCommand pgCommand = conn.CreateCommand();
                            DateTime t = DateTime.Now;
                            try
                            {
                                bool loi = false;

                                #region begin insert
                                // Insert data
                                QueryExpression exp = new QueryExpression(from);
                                exp.PageInfo = new PagingInfo();
                                exp.PageInfo.PageNumber = 1;
                                exp.PageInfo.PagingCookie = null;

                                for (int i = 0; i < a.ChildNodes.Count; i++)
                                {
                                    exp.ColumnSet.AddColumn(a.ChildNodes[i].Attributes["from"].Value);
                                    if (a.ChildNodes[i].Attributes["datatype"].Value.Trim() == "Key")
                                        key = a.ChildNodes[i].Attributes["from"].Value;
                                }

                                currentTime = DateTime.Now;

                                string newLastTime = lastTime.ToString("yyyy/MM/dd HH:mm:ss.fff");
                                string newCurrentTime = currentTime.ToString("yyyy/MM/dd HH:mm:ss.fff");
                                bool flagInserted = false;
                                bool flagUpdated = false;

                                exp.Criteria.AddCondition("createdon", ConditionOperator.Between, new string[] { newLastTime, newCurrentTime });
                                while (!loi)
                                {
                                    // Retrieve the page.

                                    EntityCollection insert = crmServices.RetrieveMultiple(exp);

                                    foreach (Entity b in insert.Entities)
                                    {
                                        #region Build insert
                                        listInsert.Add((Guid)b[key], 1);
                                        string tvalue = "";
                                        string clm = "";
                                        string keyValue = "";
                                        string keyName = "";
                                        for (int i = 0; i < a.ChildNodes.Count; i++)
                                        {
                                            string atto = a.ChildNodes[i].Attributes["to"].Value;
                                            string atfrom = a.ChildNodes[i].Attributes["from"].Value;

                                            switch (a.ChildNodes[i].Attributes["datatype"].Value.Trim())
                                            {
                                                case "Key":
                                                    clm += " \"" + atto + "\" ";
                                                    keyValue = (b.Contains(atfrom) ? ("'" + ((Guid)b[atfrom]).ToString() + "'") : "");
                                                    keyName = atto;
                                                    tvalue += (b.Contains(atfrom) ? ("'" + ((Guid)b[atfrom]).ToString() + "'") : "NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        tvalue += " , ";
                                                        clm += " , ";
                                                    }
                                                    break;
                                                case "String":
                                                    clm += " \"" + atto + "\" ";
                                                    tvalue += (b.Contains(atfrom) ? ("'" + b[atfrom].ToString() + "'") : "NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        tvalue += " , ";
                                                        clm += " , ";
                                                    }
                                                    break;
                                                case "Lookup":
                                                    clm += " \"" + atto + "\" , \"" + a.ChildNodes[i].Attributes["toname"].Value + "\" ";
                                                    tvalue += (b.Contains(atfrom) ? ("'" + ((EntityReference)b[atfrom]).Id.ToString() + "'") : "NULL");
                                                    tvalue += (b.Contains(atfrom) ? (" , '" + ((EntityReference)b[atfrom]).Name + "'") : " , NULL");

                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        tvalue += " , ";
                                                        clm += " , ";
                                                    }
                                                    break;
                                                case "Date":
                                                    clm += " \"" + atto + "\" ";
                                                    tvalue += (b.Contains(atfrom) ? ("'" + ((DateTime)b[atfrom]).ToLongDateString() + " " + ((DateTime)b[atfrom]).ToLongTimeString() + "'") : "NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        tvalue += " , ";
                                                        clm += " , ";
                                                    }
                                                    break;
                                                case "OptionSet":
                                                    clm += " \"" + atto + "\" , \"" + a.ChildNodes[i].Attributes["toname"].Value + "\"";
                                                    tvalue += (b.Contains(atfrom) ? (((OptionSetValue)b[atfrom]).Value.ToString()) : "NULL");
                                                    tvalue += (b.Contains(atfrom) ? (" , '" + GetoptionsetText(from, atfrom, ((OptionSetValue)b[atfrom]).Value) + "' ") : " , NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        tvalue += " , ";
                                                        clm += " , ";
                                                    }
                                                    break;
                                                default:
                                                    clm += " \"" + atto + "\" ";
                                                    tvalue += (b.Contains(atfrom) ? (b[atfrom].ToString()) : "NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        tvalue += " , ";
                                                        clm += " , ";
                                                    }
                                                    break;
                                            }
                                        }
                                        string tcmd = "";
                                        try
                                        {
                                            if (!string.IsNullOrEmpty(keyName) && !string.IsNullOrEmpty(keyValue))
                                            {
                                                var query = "SELECT COUNT(*) FROM \"" + to + "\" WHERE \"" + keyName + "\" = " + keyValue;
                                                var cmd = new NpgsqlCommand(query, conn);
                                                var dr = cmd.ExecuteScalar().ToString();
                                                if (dr != "0")
                                                {
                                                    continue;
                                                }
                                            }
                                            tcmd += "INSERT INTO \"" + to + "\" ( " + clm + " ) Values (" + tvalue + ")";

                                            //byte[] bytes = Encoding.Default.GetBytes(tcmd);

                                            pgCommand.CommandText = tcmd; // Encoding.UTF8.GetString(bytes);
                                            var i = pgCommand.ExecuteNonQuery();
                                            flagInserted = true;
                                        }
                                        catch (NpgsqlException ex)
                                        {
                                            Console.WriteLine("Type 0 " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\r\n" + ex.StackTrace);
                                            Console.WriteLine(SqlExceptionMessage(ex).ToString());
                                            loi = true;
                                            break;
                                        }
                                        #endregion
                                    }

                                    // Check for more records, if it returns true.
                                    if (insert.MoreRecords)
                                    {
                                        // Increment the page number to retrieve the next page.
                                        exp.PageInfo.PageNumber++;

                                        // Set the paging cookie to the paging cookie returned from current results.
                                        exp.PageInfo.PagingCookie = insert.PagingCookie;
                                    }
                                    else
                                    {
                                        // If no more records are in the result nodes, exit the loop.
                                        break;
                                    }
                                }

                                #endregion

                                #region begin update
                                exp.Criteria.Conditions.Clear();
                                newCurrentTime = DateTime.Now.AddMinutes(5).ToString("yyyy/MM/dd HH:mm:ss.fff");
                                exp.Criteria.AddCondition("modifiedon", ConditionOperator.Between, new string[] { newLastTime, newCurrentTime });
                                EntityCollection update = crmServices.RetrieveMultiple(exp);

                                foreach (Entity b in update.Entities)
                                {
                                    if (!listInsert.ContainsKey(b.Id))
                                    {
                                        string toKey = "";
                                        string updateString = "";

                                        for (int i = 0; i < a.ChildNodes.Count; i++)
                                        {
                                            string atto = a.ChildNodes[i].Attributes["to"].Value;
                                            string atfrom = a.ChildNodes[i].Attributes["from"].Value;

                                            switch (a.ChildNodes[i].Attributes["datatype"].Value.Trim())
                                            {
                                                case "Key":
                                                    toKey = atto;
                                                    break;
                                                case "String":
                                                    updateString += " \"" + atto + "\" = " + (b.Contains(atfrom) ? ("'" + b[atfrom].ToString() + "'") : " NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        updateString += " , ";
                                                    }
                                                    break;
                                                case "Lookup":
                                                    updateString += " \"" + atto + "\" =  " + (b.Contains(atfrom) ? ("'" + ((EntityReference)b[atfrom]).Id.ToString() + "'") : "NULL") +
                                                    ", \"" + a.ChildNodes[i].Attributes["toname"].Value + "\" = " + (b.Contains(atfrom) ? (" '" + ((EntityReference)b[atfrom]).Name + "'") : "NULL");

                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        updateString += " , ";
                                                    }
                                                    break;
                                                case "Date":
                                                    updateString += " \"" + atto + "\" = "
                                                     + (b.Contains(atfrom) ? ("'" + ((DateTime)b[atfrom]).ToLongDateString() + " " + ((DateTime)b[atfrom]).ToLongTimeString() + "'") : " NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        updateString += " , ";
                                                    }
                                                    break;
                                                case "OptionSet":
                                                    updateString += " \"" + atto + "\" = " + (b.Contains(atfrom) ? (((OptionSetValue)b[atfrom]).Value.ToString()) : " NULL") +
                                                        " , \"" + a.ChildNodes[i].Attributes["toname"].Value + "\" = "
                                                         + (b.Contains(atfrom) ? (" '" + GetoptionsetText(from, atfrom, ((OptionSetValue)b[atfrom]).Value) + "' ") : " NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        updateString += " , ";
                                                    }
                                                    break;
                                                default:
                                                    updateString += " \"" + atto + "\" = "
                                                     + (b.Contains(atfrom) ? ("'" + b[atfrom].ToString() + "'") : " NULL");
                                                    if (i < (a.ChildNodes.Count - 1))
                                                    {
                                                        updateString += " , ";
                                                    }
                                                    break;
                                            }
                                        }

                                        try
                                        {
                                            string tcmd = "UPDATE \"" + to + "\" SET  " + updateString +
                                            " WHERE \"" + toKey + "\" = '" + b.Id.ToString() + "'";

                                            pgCommand.CommandText = tcmd;
                                            pgCommand.ExecuteNonQuery();
                                            flagUpdated = true;
                                        }
                                        catch (NpgsqlException ex)
                                        {
                                            Console.WriteLine("Type 0.1 " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\r\n" + ex.StackTrace);
                                            Console.WriteLine(SqlExceptionMessage(ex).ToString());
                                            loi = true;
                                            break;
                                        }

                                    }
                                }

                                #endregion

                                if ((flagInserted == true || flagUpdated == true) && !loi)
                                {
                                    myTrans.Commit();
                                    //currentTime = DateTime.Now;
                                    ConfigurationManager.AppSettings.Set(synkey, currentTime.ToString("yyyy/MM/dd HH:mm:ss.fff"));
                                    xn.InnerText = currentTime.ToString("yyyy/MM/dd HH:mm:ss.fff");
                                    xkey.Save("Synkey.xml");
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Type 0.2 " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\r\n" + e.StackTrace);
                                Console.WriteLine(e.ToString());

                                myTrans.Rollback();
                            }
                            finally
                            {
                                pgCommand.Dispose();
                                myTrans.Dispose();
                                conn.Close();
                            }
                            #endregion
                        }
                        else //Client to CRM
                        {
                            bool loi = false;
                            Console.WriteLine("Syn from Client - CRM: " + from);
                            #region type 1
                            Dictionary<Guid, int> listInsert = new Dictionary<Guid, int>();

                            ExecuteMultipleRequest rqs = new ExecuteMultipleRequest()
                            {
                                Settings = new ExecuteMultipleSettings()
                                {
                                    ContinueOnError = false,
                                    ReturnResponses = true
                                },
                                // Create an empty organization request collection.
                                Requests = new OrganizationRequestCollection()
                            };

                            NpgsqlConnection conn = new NpgsqlConnection(connectString);
                            conn.Open();
                            try
                            {
                                int totalInsertPart = 100;
                                int countInsert = 0;
                                DateTime lastTimePart = new DateTime();

                                #region begin insert
                                currentTime = DateTime.Now;
                                // Insert data
                                NpgsqlCommand scmd = new NpgsqlCommand();
                                scmd.Connection = conn;
                                string selecttext = "";

                                for (int i = 0; i < a.ChildNodes.Count; i++)
                                {
                                    selecttext += "\"" + a.ChildNodes[i].Attributes["from"].Value + "\"";
                                    if (a.ChildNodes[i].Attributes["datatype"].Value.Trim() == "Key")
                                        key = a.ChildNodes[i].Attributes["from"].Value;
                                    if (i < a.ChildNodes.Count - 1)
                                        selecttext += " , ";
                                }
                                selecttext += " , " + "\"" + "CreatedDate" + "\"";

                                scmd.CommandType = CommandType.Text;
                                if (from == "PhieuDoTapChat")
                                    scmd.CommandText = "Select " + selecttext + " FROM \"" + from + "\" fr WHERE \"CreatedDate\" BETWEEN '" + lastTime.ToString("yyyy/MM/dd HH:mm:ss.fff") + "' AND '" + currentTime.ToString("yyyy/MM/dd HH:mm:ss.fff") + "' AND fr.\"KLAfter\" != 0 ORDER BY \"CreatedDate\"";
                                else
                                    scmd.CommandText = "Select " + selecttext + " FROM \"" + from + "\" WHERE \"CreatedDate\" BETWEEN '" + lastTime.ToString("yyyy/MM/dd HH:mm:ss.fff") + "' AND '" + currentTime.ToString("yyyy/MM/dd HH:mm:ss.fff") + "' ORDER BY \"CreatedDate\"";

                                DataTable insert = new DataTable();
                                NpgsqlDataAdapter adp = new NpgsqlDataAdapter(scmd);
                                adp.Fill(insert);
                                DateTime t = DateTime.Now;

                                foreach (DataRow b in insert.Rows)
                                {

                                    ++countInsert;
                                    lastTimePart = (DateTime)b["CreatedDate"];
                                    Entity record = new Entity(to);
                                    //listInsert.Add(Guid.Parse(b[key].ToString()), 1);

                                    for (int i = 0; i < a.ChildNodes.Count; i++)
                                    {
                                        #region Begin build insert sql
                                        string atto = a.ChildNodes[i].Attributes["to"].Value;
                                        string atfrom = a.ChildNodes[i].Attributes["from"].Value;

                                        switch (a.ChildNodes[i].Attributes["datatype"].Value.Trim())
                                        {
                                            case "Key":
                                                record.Id = Guid.Parse(b[key].ToString());
                                                record[to + "id"] = Guid.Parse(b[key].ToString());
                                                break;
                                            case "String":
                                                if (b[atfrom] != DBNull.Value)
                                                    record[atto] = b[atfrom].ToString();
                                                break;
                                            case "Lookup":
                                                if (b[atfrom] != DBNull.Value && (String)b[atfrom] != "" && (String)b[atfrom] != " ")
                                                    record[atto] = new EntityReference(a.ChildNodes[i].Attributes["entity"].Value, Guid.Parse(b[atfrom].ToString()));
                                                break;
                                            case "Date":
                                                if (b[atfrom] != DBNull.Value)
                                                    record[atto] = DateTime.Parse(b[atfrom].ToString());
                                                break;
                                            case "OptionSet":
                                                if (b[atfrom] != DBNull.Value)
                                                    record[atto] = new OptionSetValue(int.Parse(b[atfrom].ToString()));
                                                break;
                                            case "Money":
                                                if (b[atfrom] != DBNull.Value)
                                                    record[atto] = new Money(decimal.Parse(b[atfrom].ToString()));
                                                break;
                                            case "Boolean":
                                                if (b[atfrom] != DBNull.Value)
                                                    record[atto] = bool.Parse(b[atfrom].ToString());
                                                break;
                                            case "Number":
                                                if (b[atfrom] != DBNull.Value)
                                                    record[atto] = int.Parse(b[atfrom].ToString());
                                                break;
                                            default:
                                                if (b[atfrom] != DBNull.Value)
                                                    record[atto] = decimal.Parse(b[atfrom].ToString());
                                                break;
                                        }
                                        #endregion

                                    }
                                    //crmServices.Create(record);
                                    CreateRequest createRequest = new CreateRequest();
                                    createRequest.Target = record;
                                    rqs.Requests.Add(createRequest);
                                    t = DateTime.Parse(b["CreatedDate"].ToString());
                                    t = t.AddMilliseconds(1000);


                                    #endregion
                                    if ((countInsert % totalInsertPart) == 0 || countInsert >= insert.Rows.Count)
                                    {
                                        if (rqs.Requests.Count > 0)
                                        {
                                            ExecuteMultipleResponse responseWithResults2 = (ExecuteMultipleResponse)crmServices.Execute(rqs);

                                            foreach (ExecuteMultipleResponseItem ab in responseWithResults2.Responses)
                                            {
                                                if (ab.Fault != null)
                                                {
                                                    loi = true;
                                                    Console.WriteLine("Type 1.0 " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + ": " + ab.Fault.Message);
                                                    Console.WriteLine(ab.Fault.TraceText);
                                                    break;
                                                }
                                            }
                                            if (!loi)
                                            {
                                                ConfigurationManager.AppSettings.Set(synkey, lastTimePart.ToString("yyyy/MM/dd HH:mm:ss.fff"));
                                                xn.InnerText = t.ToString("yyyy/MM/dd HH:mm:ss.fff");
                                                xkey.Save("Synkey.xml");
                                            }
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Type 1.1 " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\r\n" + ex.StackTrace);
                                Console.WriteLine(ex.ToString());
                            }
                            conn.Close();

                            #endregion
                        }
                    }
                    #endregion
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.WriteLine("Reconnecting...");
                    do
                    {
                        try
                        {
                            crmServices = new OrganizationServiceProxy(new Uri(url), null, clientCredentials, null);
                            Console.WriteLine("Connect success!");
                        }
                        catch (Exception ex1)
                        {

                        }
                    } while (crmServices == null);
                }

                Thread.Sleep(interval);
            }
        }

        public static string GetoptionsetText(string entityName, string attributeName, int optionsetValue)
        {
            string optionsetText = string.Empty;

            RetrieveAttributeRequest retrieveAttributeRequest = new RetrieveAttributeRequest();
            retrieveAttributeRequest.EntityLogicalName = entityName;
            retrieveAttributeRequest.LogicalName = attributeName;
            retrieveAttributeRequest.RetrieveAsIfPublished = true;

            RetrieveAttributeResponse retrieveAttributeResponse =
              (RetrieveAttributeResponse)crmServices.Execute(retrieveAttributeRequest);

            OptionSetMetadata optionsetMetadata = null;
            if (retrieveAttributeResponse.AttributeMetadata.GetType() == typeof(StatusAttributeMetadata))
            {
                optionsetMetadata = ((StatusAttributeMetadata)retrieveAttributeResponse.AttributeMetadata).OptionSet;
            }
            else
            {
                PicklistAttributeMetadata picklistAttributeMetadata =
                  (PicklistAttributeMetadata)retrieveAttributeResponse.AttributeMetadata;

                optionsetMetadata = picklistAttributeMetadata.OptionSet;
            }
            foreach (OptionMetadata optionMetadata in optionsetMetadata.Options)
            {
                if (optionMetadata.Value == optionsetValue)
                {
                    optionsetText = optionMetadata.Label.UserLocalizedLabel.Label;
                    return optionsetText;
                }

            }

            return optionsetText;
        }

        public StringBuilder SqlExceptionMessage(NpgsqlException ex)
        {
            StringBuilder sqlErrorMessages = new StringBuilder("Sql Exception:\n");

            foreach (NpgsqlError error in ex.Errors)
            {
                sqlErrorMessages.AppendFormat("Mesage: {0}\n", error.Message)
                .AppendFormat("Severity level: {0}\n", error.Detail)
                .AppendFormat("Detail SQL: {0}\n", error.ErrorSql)

                .AppendLine(new string('-', error.Message.Length + 7));



            }
            return sqlErrorMessages;
        }
    }
}
