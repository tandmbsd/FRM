using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckThuadat_ChiTietHDThueDatThuaDat
{
    public class CheckThuadat_ChiTietHDThueDatThuaDat : IPlugin
    {
        //moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("new_thuadat"))
            {
                traceService.Trace("a");
                Entity chitiethdthuedat_thuadat = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_chitiethdthuedat", "new_thuadat" }));
                traceService.Trace("a1");
                Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)target["new_thuadat"]).Id,
                    new ColumnSet(new string[] { "new_diachi", "new_chusohuuchinhtd", "new_chusohuuchinhtdkhdn", "new_name" }));

                if (!chitiethdthuedat_thuadat.Contains("new_chitiethdthuedat"))
                    throw new Exception("Chi tiết hd thuê đất thửa đất không có đất thuê");

                Entity chitiethdthuedat = service.Retrieve("new_datthue", ((EntityReference)chitiethdthuedat_thuadat["new_chitiethdthuedat"]).Id,
                    new ColumnSet(new string[] { "new_hopdongthuedat", "new_benchothuedatkh", "new_benchothuedatkhdn", "new_name" }));

                if (!chitiethdthuedat.Contains("new_hopdongthuedat"))
                    throw new Exception("Đất thuê không có hợp đồng thuê đất");

                traceService.Trace("a2");
                Entity hopdongthuedat = service.Retrieve("new_hopdongthuedat", ((EntityReference)chitiethdthuedat["new_hopdongthuedat"]).Id,
                    new ColumnSet(new string[] { "new_hopdongthuedatid", "new_quocgia" }));

                if (hopdongthuedat.Contains("new_quocgia") && thuadat.Contains("new_diachi"))
                {
                    traceService.Trace("b");
                    Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadat["new_diachi"]).Id, new ColumnSet(new string[] { "new_quocgia" }));

                    if (((EntityReference)hopdongthuedat["new_quocgia"]).Id != ((EntityReference)diachi["new_quocgia"]).Id)
                    {
                        throw new Exception("Quốc gia của thửa đất và hợp đồng tư thuê đất không giống nhau !! ");
                    }
                }

                List<Entity> lstChitiethdthuedat_thuadat = RetrieveMultiRecord(service, "new_chitiethdthuedat_thuadat",
                    new ColumnSet(new string[] { "new_thuadat" }), "new_chitiethdthuedat", chitiethdthuedat.Id);

                int count = 0;

                foreach (Entity en in lstChitiethdthuedat_thuadat)
                {
                    traceService.Trace("c");
                    if (((EntityReference)en["new_thuadat"]).Id == thuadat.Id)
                    {
                        count++;
                    }
                }

                if (count > 1)
                    throw new Exception("Thửa đất đã tồn tại");

                QueryExpression q = new QueryExpression("new_hopdongthuedat");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));

                EntityCollection entc = service.RetrieveMultiple(q);
                int count1 = 0;
                traceService.Trace("d");
                foreach (Entity en in entc.Entities)
                {
                    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_datthue", new ColumnSet(new string[] { "new_datthueid" }),
                        "new_hopdongthuedat", en.Id);

                    foreach (Entity k in lstChitiet)
                    {
                        List<Entity> lstChitiethdthuedat_thuadat1 = RetrieveMultiRecord(service, "new_chitiethdthuedat_thuadat",
                    new ColumnSet(new string[] { "new_thuadat" }), "new_chitiethdthuedat", k.Id);

                        foreach (Entity td in lstChitiethdthuedat_thuadat1)
                        {
                            if (((EntityReference)td["new_thuadat"]).Id == thuadat.Id)
                                count1++;

                        }
                    }
                }

                if (count1 > 0)
                    throw new Exception("Thửa đất đã được ký ");
                traceService.Trace("e");
                if (chitiethdthuedat.Contains("new_benchothuedatkh"))
                {
                    if (thuadat.Contains("new_chusohuuchinhtdkhdn"))
                    {
                        traceService.Trace("a");
                        throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                    }
                    else if (thuadat.Contains("new_chusohuuchinhtd"))
                    {
                        if (((EntityReference)chitiethdthuedat["new_benchothuedatkh"]).Id != ((EntityReference)thuadat["new_chusohuuchinhtd"]).Id)
                        {
                            traceService.Trace("b");
                            throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                        }
                    }
                }

                else if (chitiethdthuedat.Contains("new_benchothuedatkhdn"))
                {
                    if (thuadat.Contains("new_chusohuuchinhtd"))
                    {
                        traceService.Trace("c");
                        throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                    }
                    else if (thuadat.Contains("new_chusohuuchinhtdkhdn"))
                    {
                        if (((EntityReference)chitiethdthuedat["new_benchothuedatkhdn"]).Id != ((EntityReference)thuadat["new_chusohuuchinhtdkhdn"]).Id)
                        {
                            traceService.Trace("d");
                            throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                        }
                    }
                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }
    }
}
