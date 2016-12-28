using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PDNThuong
{
    public class Plugin_PDNThuong : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            //moi nhat
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));

                Entity target = (Entity)context.InputParameters["Target"];

                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value.ToString() == "100000000" && context.Depth < 2)
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);

                    Entity pdnthuong = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_hopdongthuhoach", "new_thuadat", "new_lenhdon", "new_name", "new_dinhmucthuong", "new_klmiaduocthuong", "statuscode", "new_tienthuong", "new_loaithuong" }));
                    Entity lenhdon = new Entity(); ;
                    Entity chinhsachthumua = new Entity(); ;
                    string loaithuong = ((OptionSetValue)pdnthuong["new_loaithuong"]).Value.ToString();

                    if (loaithuong == "100000001") // thuong ti le mia chay thap
                    {
                        if (pdnthuong.Contains("new_lenhdon"))
                            lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)pdnthuong["new_lenhdon"]).Id,
                                new ColumnSet(new string[] { "new_chinhsachthumua", "new_name" }));
                        else
                            throw new Exception("Phiếu đề nghị thưởng" + pdnthuong["new_name"].ToString() + "chưa có lệnh đốn");

                        if (lenhdon.Contains("new_chinhsachthumua"))
                            chinhsachthumua = service.Retrieve("new_chinhsachthumua", ((EntityReference)lenhdon["new_chinhsachthumua"]).Id,
                                new ColumnSet(new string[] { "new_phantramtilemiachay", "new_dinhmucthuongmiachay", "new_name" }));
                        else
                            throw new Exception("Lệnh đốn " + lenhdon["new_name"].ToString() + " chưa có chính sách thu mua");

                        QueryExpression q = new QueryExpression("new_chitietnghiemthusauthuhoach");
                        q.ColumnSet = new ColumnSet(true);
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition("new_hopdongthuhoach", ConditionOperator.Equal, ((EntityReference)pdnthuong["new_hopdongthuhoach"]).Id);
                        q.Criteria.AddCondition("new_thuadat", ConditionOperator.Equal, ((EntityReference)pdnthuong["new_thuadat"]).Id);
                        EntityCollection entc = service.RetrieveMultiple(q);

                        List<Entity> chitiet_ntsauthuhoach = entc.Entities.ToList<Entity>();
                        decimal tongslmiachay = 0;
                        decimal tongsl = 0;

                        foreach (Entity en in chitiet_ntsauthuhoach)
                        {
                            tongslmiachay += en.Contains("new_miachay") ? (decimal)en["new_miachay"] : 0;
                            tongsl += en.Contains("new_tongsanluong") ? (decimal)en["new_tongsanluong"] : 0;
                        }

                        if (tongsl != 0)
                        {
                            decimal phantram = (tongslmiachay / tongsl) * 100;
                            decimal phantrammiachay = chinhsachthumua.Contains("new_phantramtilemiachay") ? (decimal)chinhsachthumua["new_phantramtilemiachay"] : 0;
                            decimal dinhmucthuong = chinhsachthumua.Contains("new_dinhmucthuongmiachay") ? ((Money)chinhsachthumua["new_dinhmucthuongmiachay"]).Value : 0;
                            Entity pdnthuong1 = service.Retrieve(pdnthuong.LogicalName, pdnthuong.Id, new ColumnSet(true));

                            if (phantram <= phantrammiachay)
                            {
                                pdnthuong1["new_dinhmucthuong"] = new Money(dinhmucthuong);
                                pdnthuong1["new_tienthuong"] = new Money(dinhmucthuong * (decimal)pdnthuong["new_klmiaduocthuong"]);
                            }
                            else
                            {
                                pdnthuong1["new_dinhmucthuong"] = new Money(0);
                                pdnthuong1["new_tienthuong"] = new Money(0);
                            }
                            service.Update(pdnthuong1);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
        }
    }
}
