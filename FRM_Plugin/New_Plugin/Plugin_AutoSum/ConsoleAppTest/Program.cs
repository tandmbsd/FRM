using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Xrm.Sdk.XmlNamespaces;
using Microsoft.Xrm.Client.Services;
using System.Configuration;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.WebServiceClient;
using System.ServiceModel;
using System.ServiceModel.Security;
using System.Windows.Forms;

namespace ConsoleAppTest
{
    class Program
    {
        static void Main(string[] args)
        {
            OrganizationService service;
            var connectionstring = GetWindowsIntegratedSecurityConnectionString();
            var serverConnection = new ServerConnection(connectionstring);

            using (service = new OrganizationService(serverConnection.CRMConnection))
            {
                Guid entityId = new Guid("67FB68B5-FBDC-E511-93F1-9ABE942A7E29"); //  7b25569F14-5DC6-E511-93F1-9ABE942A7E29
                Entity target = service.Retrieve("new_thuadatcanhtac", entityId, new ColumnSet(true));

                Console.WriteLine("Ten HD " + target["new_name"]);

                //target = (Entity)context.InputParameters["Target"];

                Entity fullEntity = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                QueryExpression q = new QueryExpression("new_autosum");
                q.ColumnSet = new ColumnSet(new string[] { "new_autosumid", "new_name", "new_phuongthuctinh", "new_childentity", "new_parentfield", "new_childfield", "new_lookupfield", "new_datatype" });
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_childentity", ConditionOperator.Equal, target.LogicalName));
                q.Criteria.AddCondition(new ConditionExpression("new_active", ConditionOperator.Equal, true));
                EntityCollection entc = service.RetrieveMultiple(q);

                Console.WriteLine("So luong entity " + entc.Entities.Count());
                MessageBox.Show("So luong entity " + entc.Entities.Count());

                Console.ReadKey();

                //if (entc.Entities.Count > 0)
                //{
                //    foreach (Entity a in entc.Entities)
                //    {
                //        QueryExpression q2 = new QueryExpression(a["new_childentity"].ToString());
                //        q2.ColumnSet = new ColumnSet(a["new_childfield"].ToString().Split(','));
                //        q2.Criteria = new FilterExpression();
                //        q2.Criteria.AddCondition(new ConditionExpression(a["new_lookupfield"].ToString(), ConditionOperator.Equal, ((EntityReference)fullEntity[a["new_lookupfield"].ToString()]).Id));
                //        EntityCollection entc2 = service.RetrieveMultiple(q2);

                //        decimal[] rs = new decimal[a["new_childfield"].ToString().Split(',').Length];
                //        int sl = entc2.Entities.Count();
                //        //int sl = a["new_childfield"].ToString().Split(',').Count();

                //        foreach (Entity b in entc2.Entities)
                //        {
                //            int i = -1;
                //            if (a["new_datatype"].ToString().ToLower().Trim() == "decimal")
                //            {
                //                foreach (string c in a["new_childfield"].ToString().Split(','))
                //                {
                //                    if (c.Trim() != "")
                //                    {
                //                        if (((OptionSetValue)a["new_phuongthuctinh"]).Value == 100000000)
                //                        {
                //                            i++;
                //                            rs[i] += (b.Attributes.Contains(c) ? (decimal)b[c] : new decimal(0));
                //                        }
                //                        if (((OptionSetValue)a["new_phuongthuctinh"]).Value == 100000001)
                //                        {
                //                            if (sl > 0)
                //                            {
                //                                i++;
                //                                rs[i] += (b.Attributes.Contains(c) ? (decimal)b[c] : new decimal(0)) / sl;
                //                            }
                //                        }
                //                    }
                //                }
                //            }
                //            else
                //            {
                //                foreach (string c in a["new_childfield"].ToString().Split(','))
                //                {
                //                    if (c.Trim() != "")
                //                    {
                //                        if (((OptionSetValue)a["new_phuongthuctinh"]).Value == 100000000)
                //                        {
                //                            i++;
                //                            rs[i] += (b.Attributes.Contains(c) ? ((Money)b[c]).Value : new decimal(0));
                //                        }
                //                        if (((OptionSetValue)a["new_phuongthuctinh"]).Value == 100000001)
                //                        {
                //                            if (sl > 0)
                //                            {
                //                                i++;
                //                                rs[i] += (b.Attributes.Contains(c) ? ((Money)b[c]).Value : new decimal(0)) / sl;
                //                            }
                //                        }
                //                    }
                //                }
                //            }
                //        }

                //        Entity Ers = new Entity(a["new_name"].ToString());
                //        Ers.Id = ((EntityReference)fullEntity[a["new_lookupfield"].ToString()]).Id;
                //        int k = -1;
                //        foreach (string c in a["new_parentfield"].ToString().Split(','))
                //            if (c.Trim() != "")
                //            {
                //                k++;
                //                if (a["new_datatype"].ToString().ToLower().Trim() == "decimal")
                //                    Ers[c] = rs[k];
                //                else
                //                    Ers[c] = new Money(rs[k]);
                //            }
                //        service.Update(Ers);
                //    }
                //}
            }//using
        }

        public static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection collRecords = crmservices.RetrieveMultiple(query);

            return collRecords;
        }

        public static string GetWindowsIntegratedSecurityConnectionString()
        {
            int count = ConfigurationManager.ConnectionStrings.Count;
            if (count == 0)
                throw new Exception("Could not find ConnectionString");

            return ConfigurationManager.ConnectionStrings[0].ConnectionString;
        }

        public static EntityCollection FindChinhSachDT(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            //OptionSetValue[] values = new OptionSetValue[] { new OptionSetValue(100000000), new OptionSetValue(100000003) };
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000000));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }
    }
}


//StringBuilder xml = new StringBuilder();
// xml.AppendLine("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
// xml.AppendLine()
//stringA.AppendLine(stringB) nghia la stringA = stringA + stringB

// muốn check field có value không thì dùng service.Trace(entityA.Attributes.Contains("fieldxyz")); 
// sau đó dùng hàm Contains() để check coi nó có value ko
// hoặc dùng if (bien == null) Trace("A") else Trace("B");

//Logger.Write("Phonecall PostCreate", "Begin");
//throw new InvalidPluginExecutionException("End");
//Logger.Write("entity Id", entity.Id.ToString());

//Convert.ChangeType(mCSTM["new_dongiatang1ccs"]), decimal);
//giá trị mới = Convert.ChangeType(val, pd.PropertyType);
//service.Trace(entityA.Attributes.Contains("fieldxyz"));
//service.Trace("vi tri 1");