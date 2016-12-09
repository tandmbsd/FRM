using System;
using System.Configuration;
using System.ServiceModel;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System.Collections.Generic;
using Microsoft.Xrm.Sdk;
using System.ServiceModel.Description;
using Microsoft.Xrm.Sdk.Client;
using System.Text;
using System.Linq;

namespace TestCRMConsole
{
    class Program
    {
        public static IOrganizationService service;
        static void Main(string[] args)
        {
            var credentials = new ClientCredentials();
            credentials.UserName.UserName = @"ttc2\crmservices";
            credentials.UserName.Password = @"P@ssword";

            Uri OrganizationUri = new Uri("http://10.33.0.58/TEST/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;
            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            {
                service = (IOrganizationService)serviceProxy;
                EntityReference target = new EntityReference("new_bangketienmia", new Guid("{BF454163-59B0-E611-80C9-9457A558474F}"));

                EntityCollection RefLenhdon = RetrieveNNRecord(service, "new_lenhdon", "new_bangketienmia", "new_new_bangketienmia_new_lenhdon", new ColumnSet(true), "new_bangketienmiaid", target.Id);
                EntityReferenceCollection entcReflenhdon = new EntityReferenceCollection();
                if (RefLenhdon.Entities.Count > 0)
                {
                    foreach (Entity en in RefLenhdon.Entities)
                        entcReflenhdon.Add(en.ToEntityReference());
                    service.Disassociate("new_bangketienmia", target.Id, new Relationship("new_new_bangketienmia_new_lenhdon"), entcReflenhdon);
                }

                Entity LenhChi = service.Retrieve("new_bangketienmia", target.Id, new ColumnSet(true));
                EntityCollection DSKHCN = RetrieveNNRecord(service, "contact", "new_bangketienmia", "new_new_bangketienmia_contact", new ColumnSet(true), "new_bangketienmiaid", target.Id);
                EntityCollection DSKHDN = RetrieveNNRecord(service, "account", "new_bangketienmia", "new_new_bangketienmia_account", new ColumnSet(true), "new_bangketienmiaid", target.Id);
                Entity Vuthuhoach = service.Retrieve("new_vuthuhoach", ((EntityReference)LenhChi["new_vuthuhoach"]).Id, new ColumnSet(true));
                Guid VuDauTu = ((EntityReference)Vuthuhoach["new_vudautu"]).Id;

                QueryExpression qe = new QueryExpression("new_lenhdon");
                qe.ColumnSet = new ColumnSet(new string[] { "new_lenhdonid", "new_khachhang", "new_khachhangdoanhnghiep" });
                FilterExpression ft1 = new FilterExpression(LogicalOperator.And);
                ft1.Conditions.Add(new ConditionExpression("new_thoigiancanra", ConditionOperator.OnOrAfter, ((DateTime)LenhChi["new_tungay"]).AddHours(7)));
                ft1.Conditions.Add(new ConditionExpression("new_thoigiancanra", ConditionOperator.OnOrBefore, ((DateTime)LenhChi["new_denngay"]).AddHours(7)));
                ft1.Conditions.Add(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000002));
                ft1.Conditions.Add(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VuDauTu));

                if (DSKHCN.Entities.Count > 0 || DSKHDN.Entities.Count > 0)
                {
                    FilterExpression ft2 = new FilterExpression(LogicalOperator.Or);
                    if (DSKHCN.Entities.Count > 0)
                        ft2.Conditions.Add(new ConditionExpression("new_khachhang", ConditionOperator.In, DSKHCN.Entities.Select(o => o["contactid"]).ToList()));
                    if (DSKHDN.Entities.Count > 0)
                        ft2.Conditions.Add(new ConditionExpression("new_khachhangdoanhnghiep", ConditionOperator.In, DSKHDN.Entities.Select(o => o["accountid"]).ToList()));
                    ft1.AddFilter(ft2);
                }

                qe.Criteria = ft1;

                EntityCollection result = service.RetrieveMultiple(qe);

                if (result.Entities.Count > 0)
                {
                    EntityReferenceCollection RefferLenhdon = new EntityReferenceCollection();
                    EntityReferenceCollection RefferKHCN = new EntityReferenceCollection();
                    EntityReferenceCollection RefferKHDN = new EntityReferenceCollection();

                    List<Guid> IDCN = new List<Guid>();
                    List<Guid> IDDN = new List<Guid>();

                    foreach (Entity en in result.Entities)
                    {
                        RefferLenhdon.Add(en.ToEntityReference());
                        if (en.Contains("new_khachhang"))
                            if (IDCN.IndexOf(((EntityReference)en["new_khachhang"]).Id) == -1)
                            {
                                RefferKHCN.Add((EntityReference)en["new_khachhang"]);
                                IDCN.Add(((EntityReference)en["new_khachhang"]).Id);
                            }
                        if (en.Contains("new_khachhangdoanhnghiep"))
                            if (IDDN.IndexOf(((EntityReference)en["new_khachhangdoanhnghiep"]).Id) == -1)
                            {
                                RefferKHDN.Add((EntityReference)en["new_khachhangdoanhnghiep"]);
                                IDDN.Add(((EntityReference)en["new_khachhangdoanhnghiep"]).Id);
                            }
                    }

                    service.Associate("new_bangketienmia", target.Id, new Relationship("new_new_bangketienmia_new_lenhdon"), RefferLenhdon);

                    if (DSKHCN.Entities.Count == 0 && DSKHDN.Entities.Count == 0)
                    {
                        if (RefferKHCN.Count > 0)
                            service.Associate("new_bangketienmia", target.Id, new Relationship("new_new_bangketienmia_contact"), RefferKHCN);
                        if (RefferKHDN.Count > 0)
                            service.Associate("new_bangketienmia", target.Id, new Relationship("new_new_bangketienmia_account"), RefferKHDN);
                    }
                }

            }
        }

        static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string entity2condition, object entity2value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(entity2condition, ConditionOperator.Equal, entity2value));
            EntityCollection collRecords = service.RetrieveMultiple(query);

            return collRecords;
        }
    }
}
