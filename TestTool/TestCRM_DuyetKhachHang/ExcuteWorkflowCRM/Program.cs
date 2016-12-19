using System;
using System.Configuration;
using System.ServiceModel;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System.Collections.Generic;
using Microsoft.Xrm.Sdk;
using System.ServiceModel.Description;
using Microsoft.Xrm.Sdk.Client;

namespace TestCRM_Oracle
{
    class Program
    {
        public static IOrganizationService service;
        static void Main(string[] args)
        {
            var credentials = new ClientCredentials();
            credentials.UserName.UserName = @"ttc2\crmservices";
            credentials.UserName.Password = @"P@ssword";

            Uri OrganizationUri = new Uri("http://10.33.0.58/TTCS/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;
            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            {
                List<string> listMaKH = new List<string>() {
                    "018254",
"018249",
"018223",
"018222",
"018212",
"017888",
"017868",
"017822",
"017783",
"017782",
"017752",
"017751",
"017750",
"017704",
"017696",
"017673",
"017622",
"017609",
"017554",
"017550",
"017549",
"017540",
"017536",
"017519",
"017500",
"017492",
"017484",
"017483",
"017482",
"017474",
"017471",
"017470",
"017468",
"017465",
"017461",
"017460",
"017456",
"017454",
"017444",
"017430",
"017241",
"017140",
"017133",
"017091",
"016349",
"013706",
"013050",
"DN000105",
"012412"

                };

                service = (IOrganizationService)serviceProxy;

                QueryExpression query = new QueryExpression("contact");
                query.Criteria = new FilterExpression();
                query.Criteria.AddCondition("new_makhachhang", ConditionOperator.In, listMaKH);
                query.ColumnSet = new ColumnSet(new string[] { "statuscode", "new_makhachhang" });

                var result = service.RetrieveMultiple(query);

                foreach (Entity entity in result.Entities)
                {
                    entity["statuscode"] = new OptionSetValue(1);
                    service.Update(entity);
                    entity["statuscode"] = new OptionSetValue(100000000);
                    service.Update(entity);
                    Console.WriteLine(entity["new_makhachhang"]);
                }

                query = new QueryExpression("account");
                query.Criteria = new FilterExpression();
                query.Criteria.AddCondition("new_makhachhang", ConditionOperator.In, listMaKH);
                query.ColumnSet = new ColumnSet(new string[] { "statuscode", "new_makhachhang" });

                var resultDN = service.RetrieveMultiple(query);

                foreach (Entity entity in resultDN.Entities)
                {
                    entity["statuscode"] = new OptionSetValue(1);
                    service.Update(entity);
                    entity["statuscode"] = new OptionSetValue(100000000);
                    service.Update(entity);
                    Console.WriteLine(entity["new_makhachhang"]);
                }

                Console.WriteLine("het");
                Console.ReadLine();
            }
        }

    }
}
