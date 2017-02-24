using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Text;

namespace Plugin_Ghepchuoimucdichsudung
{
    public class Plugin_Ghepchuoimucdichsudung : IPlugin
    {
        private IOrganizationService service;
        private IOrganizationServiceFactory serviceProxy;
        private ITracingService trace = null;

        public void Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            serviceProxy = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = serviceProxy.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            var target = (Entity)context.InputParameters["Target"];
            Entity mucdichsudung = service.Retrieve("new_mucdichsudung", target.Id,
                new ColumnSet(new string[] { "new_taisan", "new_sotobando", "new_sothua", "new_phanloaidat" }));

            trace.Trace("1");
            List<string> daythuadat = new List<string>();
            List<string> daytobando = new List<string>();
            List<string> daymucdichsudung = new List<string>();
            List<string> daythoihan = new List<string>();

            Entity taisan = service.Retrieve("new_taisan", ((EntityReference)mucdichsudung["new_taisan"]).Id,
                new ColumnSet(new string[] { "new_daythuadat", "new_daytobando", "new_daymucdichsudung" }));

            List<Entity> lstMucdichsudung = RetrieveMultiRecord(service, "new_mucdichsudung",
                new ColumnSet(new string[] { "new_taisan", "new_sotobando", "new_sothua", "new_phanloaidat","new_thoihan" }), "new_taisan", taisan.Id);
            trace.Trace("2");
            foreach (Entity en in lstMucdichsudung)
            {
                string sotobando = en.Contains("new_sotobando") ? (string)en["new_sotobando"] : "";
                string sothua = en.Contains("new_sothua") ? (string)en["new_sothua"] : "";
                string phanloaidat = en.Contains("new_phanloaidat") ? en.FormattedValues["new_phanloaidat"].ToString() : "";
                string thoihan = en.Contains("new_thoihan") ? (string) en["new_thoihan"] : "";

                if (sotobando != "" && Duplicate(daytobando, sotobando) == false)
                    daytobando.Add(sotobando);

                if (sothua != "" && Duplicate(daythuadat, sothua) == false)
                    daythuadat.Add(sothua);

                if (phanloaidat != "" && Duplicate(daymucdichsudung, phanloaidat) == false)
                    daymucdichsudung.Add(phanloaidat);

                if (thoihan != "" && Duplicate(daythoihan, thoihan) == false)
                    daythoihan.Add(thoihan);
            }
            trace.Trace("3");
            StringBuilder a = new StringBuilder();
            StringBuilder b = new StringBuilder();
            StringBuilder c = new StringBuilder();
            StringBuilder d = new StringBuilder();

            for (int i = 0; i < daythuadat.Count; i++)
            {
                if (i == 0)
                    a.Append(daythuadat[i]);
                else
                    a.Append("," + daythuadat[i]);
            }

            for (int i = 0; i < daytobando.Count; i++)
            {
                if (i == 0)
                    b.Append(daytobando[i]);
                else
                    b.Append("," + daytobando[i]);
            }

            for (int i = 0; i < daymucdichsudung.Count; i++)
            {
                if (i == 0)
                    c.Append(daymucdichsudung[i]);
                else
                    c.Append("," + daymucdichsudung[i]);
            }

            for (int i = 0; i < daythoihan.Count; i++)
            {
                if (i == 0)
                    d.Append(daythoihan[i]);
                else
                    d.Append("," + daythoihan[i]);
            }

            taisan["new_daythuadat"] = a.ToString();
            taisan["new_daytobando"] = b.ToString();
            taisan["new_daymucdichsudung"] = c.ToString();
            taisan["new_daythoihan"] = d.ToString();

            service.Update(taisan);
        }

        private List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column,
            string condition, object value)
        {
            var q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            var entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList();
        }

        bool Duplicate(List<string> lst, string temp)
        {
            if (lst.Contains(temp))
                return true;
            return false;
        }
    }
}
