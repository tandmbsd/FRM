using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_ChangStatusChitietHDOnThamDinh
{
    public class Plugin_ChangStatusChitietHDOnThamDinh : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode"))
            {
                Entity PostImg = (Entity)context.PostEntityImages["PostImg"];
                if (!PostImg.Contains("new_hopdongdautumia")) throw new Exception("Không có thông tin hợp đồng, vui lòng thử lại !");
                Entity HDMia = service.Retrieve("new_hopdongdautumia", ((EntityReference)PostImg["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "statuscode" }));
                if (((OptionSetValue)HDMia["statuscode"]).Value == 100000005)
                {
                    QueryExpression q = new QueryExpression("new_phieuthamdinhdautu");
                    q.ColumnSet = new ColumnSet(new string[] { "new_phieuthamdinhdautuid" });
                    q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 1));
                    q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, HDMia.Id));

                    EntityCollection result = service.RetrieveMultiple(q);
                    if (result.Entities.Count > 0)
                        AutoSum(service, result[0].Id, HDMia.Id);
                }
            }
        }

        void AutoSum(IOrganizationService service, Guid PTDId, Guid HDmia)
        {
            StringBuilder fetchXML = new StringBuilder();
            fetchXML.AppendFormat("<fetch mapping='logical' distinct='false' aggregate='true' version='1.0'>");
            fetchXML.AppendFormat("<entity name='new_thuadatcanhtac'>");
            fetchXML.AppendFormat("<attribute name='new_dientichhopdong' aggregate='sum' alias='v_new_dientichhopdong'/>");
            fetchXML.AppendFormat("<attribute name='new_loaigocmia' alias='new_loaigocmia' groupby='true'/>");
            fetchXML.AppendFormat("<attribute name='statuscode' alias='statuscode' groupby='true'/>");
            fetchXML.AppendFormat("<filter type='and'>");
            fetchXML.AppendFormat("<condition attribute='new_hopdongdautumia' operator='eq' value='{0}' />", HDmia.ToString());
            fetchXML.AppendFormat("<condition attribute='statecode' operator='eq' value='{0}' />", 0);
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("</entity>");
            fetchXML.AppendFormat("</fetch>");

            decimal Dtotal = 0;
            decimal Dt_to = 0;
            decimal Dt_goc = 0;

            decimal Ktotal = 0;
            decimal Kt_to = 0;
            decimal Kt_goc = 0;

            EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXML.ToString()));
            if (result.Entities.Count > 0)
            {
                foreach (Entity a in result.Entities)
                {
                    if (((OptionSetValue)((AliasedValue)a["statuscode"]).Value).Value == 1)
                    {
                        if (((OptionSetValue)((AliasedValue)a["new_loaigocmia"]).Value).Value == 100000000)
                        {
                            Dtotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                            Dt_to += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                        }
                        else
                        {
                            Dtotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                            Dt_goc += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                        }
                    }
                    else if (((OptionSetValue)((AliasedValue)a["statuscode"]).Value).Value == 100000008)
                    {
                        if (((OptionSetValue)((AliasedValue)a["new_loaigocmia"]).Value).Value == 100000000)
                        {
                            Ktotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                            Kt_to += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                        }
                        else
                        {
                            Ktotal += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                            Kt_goc += (((AliasedValue)a["v_new_dientichhopdong"]).Value == null ? 0 : (decimal)((AliasedValue)a["v_new_dientichhopdong"]).Value);
                        }
                    }
                }
            }

            Entity PTD = new Entity("new_phieuthamdinhdautu");
            PTD.Id = PTDId;
            PTD["new_tongdientich_chapthuan"] = Dtotal;
            PTD["new_trongmoi_chapthuan"] = Dt_to;
            PTD["new_chamsocgoc_chapthuan"] = Dt_goc;
            PTD["new_tongdientich_khongchapthuan"] = Ktotal;
            PTD["new_trongmoi_khongchapthuan"] = Kt_to;
            PTD["new_chamsocgoc_khongchapthuan"] = Kt_goc;
            service.Update(PTD);
        }
    }
}
