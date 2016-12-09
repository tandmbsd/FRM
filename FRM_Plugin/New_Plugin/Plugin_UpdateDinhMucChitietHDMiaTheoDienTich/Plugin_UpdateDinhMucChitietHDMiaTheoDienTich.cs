using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_UpdateDinhMucChitietHDMiaTheoDienTich
{
    public class Plugin_UpdateDinhMucChitietHDMiaTheoDienTich : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) //Da ky HD
            {
                Entity ctHDmia = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(new string[] { "new_dongiahopdong", "new_dongiahopdongkhl", "new_dongiaphanbonhd", "new_dientichhopdong", "new_dientichthucte", "new_dientichconlai" }));
                Entity cthdmianew = new Entity("new_thuadatcanhtac");
                cthdmianew.Id = ctHDmia.Id;
                cthdmianew["new_dautuhoanlai"] = new Money((ctHDmia.Contains("new_dongiahopdong") ? ((Money)ctHDmia["new_dongiahopdong"]).Value : 0) * (ctHDmia.Contains("new_dientichhopdong") ? (decimal)ctHDmia["new_dientichhopdong"] : 0));
                cthdmianew["new_dautukhonghoanlai"] = new Money((ctHDmia.Contains("new_dongiahopdongkhl") ? ((Money)ctHDmia["new_dongiahopdongkhl"]).Value : 0) * (ctHDmia.Contains("new_dientichhopdong") ? (decimal)ctHDmia["new_dientichhopdong"] : 0));
                cthdmianew["new_tongchiphidautu"] = new Money(((Money)cthdmianew["new_dautuhoanlai"]).Value + ((Money)cthdmianew["new_dautukhonghoanlai"]).Value);
                cthdmianew["new_sotienphanbontoithieu"] = new Money((ctHDmia.Contains("new_dongiaphanbonhd") ? ((Money)ctHDmia["new_dongiaphanbonhd"]).Value : 0) * (ctHDmia.Contains("new_dientichhopdong") ? (decimal)ctHDmia["new_dientichhopdong"] : 0));
                cthdmianew["new_dientichconlai"] = ctHDmia.Contains("new_dientichhopdong") ? ctHDmia["new_dientichhopdong"] : (decimal)0 ;

                service.Update(cthdmianew);
            }

            if (target.Contains("new_dientichthucte") || (target.Contains("new_trangthainghiemthu") && ((OptionSetValue)target["new_trangthainghiemthu"]).Value >= 100000002)) //NT lần 1
            {
                Entity ctHDmia = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(new string[] { "new_dongiahopdong", "new_dongiahopdongkhl", "new_dongiaphanbonhd", "new_dientichhopdong", "new_dientichthucte", "new_dientichconlai" }));
                Entity cthdmianew = new Entity("new_thuadatcanhtac");
                cthdmianew.Id = target.Id;
                cthdmianew["new_dientichconlai"] = ctHDmia.Contains("new_dientichthucte") ? ctHDmia["new_dientichthucte"] : (decimal)0;
                service.Update(cthdmianew);
            }

            if (target.Contains("new_dientichhopdong") || target.Contains("new_dongiahopdong") || target.Contains("new_dongiahopdongkhl") || target.Contains("new_dongiaphanbonhd"))
            {
                Entity ctHDmia = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(new string[] { "statuscode", "new_dongiahopdong", "new_dongiahopdongkhl", "new_dongiaphanbonhd", "new_dientichhopdong", "new_dientichthucte", "new_dientichconlai" }));
                Entity cthdmianew = new Entity("new_thuadatcanhtac");
                cthdmianew.Id = ctHDmia.Id;
                cthdmianew["new_dautuhoanlai"] = new Money((ctHDmia.Contains("new_dongiahopdong") ? ((Money)ctHDmia["new_dongiahopdong"]).Value : 0) * (ctHDmia.Contains("new_dientichhopdong") ? (decimal)ctHDmia["new_dientichhopdong"] : 0));
                cthdmianew["new_dautukhonghoanlai"] = new Money((ctHDmia.Contains("new_dongiahopdongkhl") ? ((Money)ctHDmia["new_dongiahopdongkhl"]).Value : 0) * (ctHDmia.Contains("new_dientichhopdong") ? (decimal)ctHDmia["new_dientichhopdong"] : 0));
                cthdmianew["new_tongchiphidautu"] = new Money(((Money)cthdmianew["new_dautuhoanlai"]).Value + ((Money)cthdmianew["new_dautukhonghoanlai"]).Value);
                cthdmianew["new_sotienphanbontoithieu"] = new Money((ctHDmia.Contains("new_dongiaphanbonhd") ? ((Money)ctHDmia["new_dongiaphanbonhd"]).Value : 0) * (ctHDmia.Contains("new_dientichhopdong") ? (decimal)ctHDmia["new_dientichhopdong"] : 0));
                if (((OptionSetValue)ctHDmia["statuscode"]).Value == 1)
                {
                    cthdmianew["new_dientichconlai"] = ctHDmia.Contains("new_dientichhopdong") ?  ctHDmia["new_dientichhopdong"] : (decimal)0;
                }

                service.Update(cthdmianew);
            }

            if (target.Contains("new_dientichthucte"))
            {
                Entity ctHDmia = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(new string[] { "new_dongiahopdong", "new_dongiahopdongkhl", "new_dongiaphanbonhd", "new_dientichhopdong", "new_dientichthucte", "new_dientichconlai" }));
                Entity cthdmianew = new Entity("new_thuadatcanhtac");
                cthdmianew.Id = ctHDmia.Id;

                cthdmianew["new_dinhmucdautuhoanlai_hientai"] = new Money((ctHDmia.Contains("new_dongiahopdong") ? ((Money)ctHDmia["new_dongiahopdong"]).Value : 0) * (ctHDmia.Contains("new_dientichthucte") ? (decimal)ctHDmia["new_dientichthucte"] : 0));
                cthdmianew["new_dinhmucdautukhonghoanlai_hientai"] = new Money((ctHDmia.Contains("new_dongiahopdongkhl") ? ((Money)ctHDmia["new_dongiahopdongkhl"]).Value : 0) * (ctHDmia.Contains("new_dientichthucte") ? (decimal)ctHDmia["new_dientichthucte"] : 0));
                cthdmianew["new_dinhmucphanbontt"] = new Money((ctHDmia.Contains("new_dongiaphanbonhd") ? ((Money)ctHDmia["new_dongiaphanbonhd"]).Value : 0) * (ctHDmia.Contains("new_dientichthucte") ? (decimal)ctHDmia["new_dientichthucte"] : 0));

                service.Update(cthdmianew);
            }

            if (target.Contains("new_dientichconlai"))
            {
                Entity ctHDmia = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(new string[] { "new_dongiahopdong", "new_dongiahopdongkhl", "new_dongiaphanbonhd", "new_dientichhopdong", "new_dientichthucte", "new_dientichconlai" }));
                Entity cthdmianew = new Entity("new_thuadatcanhtac");
                cthdmianew.Id = ctHDmia.Id;

                cthdmianew["new_conlai_hoanlai"] = new Money((ctHDmia.Contains("new_dongiahopdong") ? ((Money)ctHDmia["new_dongiahopdong"]).Value : 0) * (ctHDmia.Contains("new_dientichconlai") ? (decimal)ctHDmia["new_dientichconlai"] : 0));
                cthdmianew["new_conlai_khonghoanlai"] = new Money((ctHDmia.Contains("new_dongiahopdongkhl") ? ((Money)ctHDmia["new_dongiahopdongkhl"]).Value : 0) * (ctHDmia.Contains("new_dientichconlai") ? (decimal)ctHDmia["new_dientichconlai"] : 0));
                cthdmianew["new_conlai_phanbontoithieu"] = new Money((ctHDmia.Contains("new_dongiaphanbonhd") ? ((Money)ctHDmia["new_dongiaphanbonhd"]).Value : 0) * (ctHDmia.Contains("new_dientichconlai") ? (decimal)ctHDmia["new_dientichconlai"] : 0));

                service.Update(cthdmianew);
            }
        }
    }
}
