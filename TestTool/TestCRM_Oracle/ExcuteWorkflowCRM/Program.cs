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
         
            Uri OrganizationUri = new Uri("http://10.33.0.58/TEST/XRMServices/2011/Organization.svc");
            Uri HomeRealmUri = null;
            using (OrganizationServiceProxy serviceProxy = new OrganizationServiceProxy(OrganizationUri, HomeRealmUri, credentials, null))
            {
                service = (IOrganizationService)serviceProxy;

                Dictionary<string, string> data = new Dictionary<string, string>();
                /*data.Add("13F418C9-2BB9-E611-80CA-9457A558474F", "new_phieudenghithanhtoan");
                data.Add("142927E2-7DB8-E611-80CA-9457A558474F", "new_phieugiaonhanvattu");
                data.Add("1CDF5EBC-7EB8-E611-80CA-9457A558474F", "new_phieudenghithanhtoan");
                data.Add("447B13A8-79B8-E611-80CA-9457A558474F", "new_phieudenghigiaingan");
                data.Add("53EBA3D8-7CB8-E611-80CA-9457A558474F", "new_phieugiaonhanphanbon");
                data.Add("616BFDF6-7FB8-E611-80CA-9457A558474F", "new_phieudenghithanhtoan");
                data.Add("649ECCE5-82B8-E611-80CA-9457A558474F", "new_phieudenghithanhtoan");
                data.Add("6AE2B818-7CB8-E611-80CA-9457A558474F", "new_phieugiaonhanhomgiong");
                data.Add("74BCD0C0-2DB9-E611-80CA-9457A558474F", "new_phieudenghithuno");
                data.Add("83E73575-7AB8-E611-80CA-9457A558474F", "new_phieutamung");
                data.Add("9D097418-7AB8-E611-80CA-9457A558474F", "new_phieutamung");
                data.Add("A0AA4636-2DB9-E611-80CA-9457A558474F", "new_phieudenghithanhtoan");
                data.Add("DA7483FA-78B8-E611-80CA-9457A558474F", "new_phieudenghigiaingan");
                data.Add("DC154153-31B9-E611-80CA-9457A558474F", "new_bangketienmia");
                data.Add("E431D3C2-80B8-E611-80CA-9457A558474F", "new_phieudenghithanhtoan");
                data.Add("EBF1FD49-7BB8-E611-80CA-9457A558474F", "new_phieugiaonhanhomgiong");
                data.Add("F7C41861-7DB8-E611-80CA-9457A558474F", "new_phieugiaonhanthuoc");*/

                
                data.Add("74BCD0C0-2DB9-E611-80CA-9457A558474F", "new_phieudenghithuno");
                

                foreach (var a in data)
                {
                    Console.WriteLine(a.Value);
                    Entity tmp = new Entity(a.Value);
                    tmp.Id = Guid.Parse(a.Key);
                    tmp["statuscode"] = new OptionSetValue(1);
                    service.Update(tmp);
                    tmp["statuscode"] = new OptionSetValue(100000000);
                    service.Update(tmp);
                }

                Console.WriteLine("het");
                Console.ReadLine();
            }
        }

    }
}
