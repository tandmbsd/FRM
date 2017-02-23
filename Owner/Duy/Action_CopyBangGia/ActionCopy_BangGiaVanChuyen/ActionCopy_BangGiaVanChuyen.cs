using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopy_BangGiaVanChuyen
{
    public class ActionCopy_BangGiaVanChuyen : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            EntityReference target = (EntityReference)context.InputParameters["Target"];
            string input = (string)context.InputParameters["VudautuID"];
            Guid vudautuid = new Guid(input);

            Entity vudautu = service.Retrieve("new_vudautu", vudautuid, new ColumnSet(new string[] { "new_vudautuid" }));

            if (target.LogicalName == "new_banggiavanchuyen" && vudautuid != null)
            {

                Entity bgvanchuyen = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));               

                if (bgvanchuyen == null)
                {
                    throw new Exception("Bảng giá vận chuyển này không tồn tại !!");
                }
                Entity new_bgvanchuyen = new Entity("new_banggiavanchuyen");

                string newName = bgvanchuyen.Contains("new_name") ? bgvanchuyen["new_name"].ToString() : "";
                new_bgvanchuyen["new_name"] = "New - " + newName;
                new_bgvanchuyen["new_thoidiemapdung"] = bgvanchuyen["new_thoidiemapdung"];
                //new_bgvanchuyen["new_vudautu"] = FindVudautu((EntityReference)bgvanchuyen["new_vudautu"]);
                new_bgvanchuyen["new_vudautu"] = new EntityReference("new_vudautu", vudautu.Id);
                new_bgvanchuyen["statuscode"] = new OptionSetValue(1);
                new_bgvanchuyen["new_chiphidau"] = bgvanchuyen.Contains("new_chiphidau") ? bgvanchuyen["new_chiphidau"] : null;
                new_bgvanchuyen["new_chiphiluongtaixe"] = bgvanchuyen.Contains("new_chiphiluongtaixe") ? bgvanchuyen["new_chiphiluongtaixe"] : null;
                new_bgvanchuyen["new_khauhaoxe"] = bgvanchuyen.Contains("new_khauhaoxe") ? bgvanchuyen["new_khauhaoxe"] : null;
                new_bgvanchuyen["new_chiphibaotri"] = bgvanchuyen.Contains("new_chiphibaotri") ? bgvanchuyen["new_chiphibaotri"] : null;
                new_bgvanchuyen["new_chiphiboiduongtrenduong"] = bgvanchuyen.Contains("new_chiphiboiduongtrenduong") ? bgvanchuyen["new_chiphiboiduongtrenduong"] : null;
                new_bgvanchuyen["new_loinhuanuoctinhchochuxe"] = bgvanchuyen.Contains("new_loinhuanuoctinhchochuxe") ? bgvanchuyen["new_loinhuanuoctinhchochuxe"] : null;
                new_bgvanchuyen["new_sokmdautien"] = bgvanchuyen.Contains("new_sokmdautien") ? bgvanchuyen["new_sokmdautien"] : null;
                new_bgvanchuyen["new_dongiachosokmdautien_cal"] = bgvanchuyen.Contains("new_dongiachosokmdautien") ? bgvanchuyen["new_dongiachosokmdautien"] : null;
                new_bgvanchuyen["new_chiphixangdautangthem"] = bgvanchuyen.Contains("new_chiphixangdautangthem") ? bgvanchuyen["new_chiphixangdautangthem"] : null;
                new_bgvanchuyen["new_chiphiluongtaixetangthem"] = bgvanchuyen.Contains("new_chiphiluongtaixetangthem") ? bgvanchuyen["new_chiphiluongtaixetangthem"] : null;
                new_bgvanchuyen["new_chiphibaotritangthem"] = bgvanchuyen.Contains("new_chiphibaotritangthem") ? bgvanchuyen["new_chiphibaotritangthem"] : null;
                new_bgvanchuyen["new_loinhuantangthem"] = bgvanchuyen.Contains("new_loinhuantangthem") ? bgvanchuyen["new_loinhuantangthem"] : null;

                Guid new_bgvanchuyenID = service.Create(new_bgvanchuyen);

                List<Entity> lsthesodieuchinhxangdau = RetrieveMultiRecord(service, "new_hesodieuchinhxangdau", new ColumnSet(true), "new_banggiavanchuyen", bgvanchuyen.Id);
                if (lsthesodieuchinhxangdau.Count > 0)
                {
                    foreach (Entity en in lsthesodieuchinhxangdau)
                    {
                        Entity newHSDieuchinhxangdau = new Entity("new_hesodieuchinhxangdau");
                        newHSDieuchinhxangdau["new_name"] = en["new_name"];

                        Entity bgvanchuyenEn = new Entity("new_banggiavanchuyen");
                        bgvanchuyenEn.Id = new_bgvanchuyenID;
                        newHSDieuchinhxangdau["new_banggiavanchuyen"] = bgvanchuyenEn.ToEntityReference();
                        newHSDieuchinhxangdau["new_ngayapdung"] = en["new_ngayapdung"];
                        newHSDieuchinhxangdau["new_giaxang"] = en["new_giaxang"];

                        service.Create(newHSDieuchinhxangdau);
                    }
                }

                List<Entity> lsthesodieuchinhkhuvuc = RetrieveMultiRecord(service, "new_hesodieuchinhtheokhuvuc", new ColumnSet(true), "new_banggiavanchuyen", bgvanchuyen.Id);
                if (lsthesodieuchinhkhuvuc.Count > 0)
                {
                    foreach (Entity en in lsthesodieuchinhkhuvuc)
                    {
                        Entity newHSDieuchinhkhuvuc = new Entity("new_hesodieuchinhtheokhuvuc");
                        newHSDieuchinhkhuvuc["new_name"] = en["new_name"];

                        Entity bgvanchuyenEn = new Entity("new_banggiavanchuyen");
                        bgvanchuyenEn.Id = new_bgvanchuyenID;
                        newHSDieuchinhkhuvuc["new_banggiavanchuyen"] = bgvanchuyenEn.ToEntityReference();
                        newHSDieuchinhkhuvuc["new_ngayapdung"] = en["new_ngayapdung"];
                        newHSDieuchinhkhuvuc["new_vungdialy"] = en["new_vungdialy"];
                        newHSDieuchinhkhuvuc["new_heso"] = en["new_heso"];

                        service.Create(newHSDieuchinhkhuvuc);
                    }
                }

                context.OutputParameters["ReturnId"] = new_bgvanchuyenID.ToString();
            }
        }

        EntityReference FindVudautu(EntityReference vudautuRef)
        {
            Entity CurrVudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(true));
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection entc = service.RetrieveMultiple(q);

            List<Entity> lst = entc.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == CurrVudautu.Id);
            if (curr == lst.Count - 1)
            {
                throw new Exception("Không tồn tại vụ đầu tư mới hơn !!!");
            }
            return lst[curr + 1].ToEntityReference();

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
