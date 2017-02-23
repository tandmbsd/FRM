using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Text;

namespace PTTM_Phuongthucthanhtoan
{
    public class PTTM_Phuongthucthanhtoan : IPlugin
    {
        private IOrganizationService service;
        private IOrganizationServiceFactory factory;
        private ITracingService trace;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = context.InputParameters["Target"] as Entity;

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000 && context.Depth < 2) // hoan tat
            {
                Entity chumia = null;
                Entity vanchuyen = null;
                Entity thuhoach = null;

                Entity pttm = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep",
                        "new_doitacvanchuyen","new_doitacvanchuyenkhdn","new_doitacthuhoach","new_doitacthuhoachkhdn" }));

                if (pttm.Contains("new_khachhang"))
                {
                    chumia = service.Retrieve("contact", ((EntityReference)pttm["new_khachhang"]).Id,
                        new ColumnSet(new string[] { "new_phuongthucthanhtoan" }));
                }
                else if (pttm.Contains("new_khachhangdoanhnghiep"))
                {
                    chumia = service.Retrieve("account", ((EntityReference)pttm["new_khachhangdoanhnghiep"]).Id,
                        new ColumnSet(new string[] { "new_phuongthucthanhtoan" }));
                }

                if (pttm.Contains("new_doitacvanchuyen"))
                {
                    vanchuyen = service.Retrieve("contact", ((EntityReference)pttm["new_doitacvanchuyen"]).Id,
                        new ColumnSet(new string[] { "new_phuongthucthanhtoan" }));
                }
                else if (pttm.Contains("new_doitacvanchuyenkhdn"))
                {
                    vanchuyen = service.Retrieve("account", ((EntityReference)pttm["new_doitacvanchuyenkhdn"]).Id,
                        new ColumnSet(new string[] { "new_phuongthucthanhtoan" }));
                }

                if (pttm.Contains("new_doitacthuhoach"))
                {
                    thuhoach = service.Retrieve("contact", ((EntityReference)pttm["new_doitacthuhoach"]).Id,
                        new ColumnSet(new string[] { "new_phuongthucthanhtoan" }));
                }
                else if (pttm.Contains("new_doitacthuhoachkhdn"))
                {
                    thuhoach = service.Retrieve("account", ((EntityReference)pttm["new_doitacthuhoachkhdn"]).Id,
                        new ColumnSet(new string[] { "new_phuongthucthanhtoan" }));
                }

                int ptttChumia = chumia.Contains("new_phuongthucthanhtoan") ? ((OptionSetValue)chumia["new_phuongthucthanhtoan"]).Value : 0;
                int ptttVanchuyen = vanchuyen.Contains("new_phuongthucthanhtoan") ? ((OptionSetValue)vanchuyen["new_phuongthucthanhtoan"]).Value : 0;
                int ptttThuhoach = thuhoach.Contains("new_phuongthucthanhtoan") ? ((OptionSetValue)thuhoach["new_phuongthucthanhtoan"]).Value : 0;

                Entity pttmUpdate = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_phuongthucthanhtoanchumia", "new_phuongthucthanhtoanvanchuyen",
                        "new_phuongthucthanhtoanthuhoach" }));

                pttmUpdate["new_phuongthucthanhtoanchumia"] = new OptionSetValue(ptttChumia);
                pttmUpdate["new_phuongthucthanhtoanvanchuyen"] = new OptionSetValue(ptttVanchuyen);
                pttmUpdate["new_phuongthucthanhtoanthuhoach"] = new OptionSetValue(ptttThuhoach);

                service.Update(pttmUpdate);
            }
        }
    }
}
