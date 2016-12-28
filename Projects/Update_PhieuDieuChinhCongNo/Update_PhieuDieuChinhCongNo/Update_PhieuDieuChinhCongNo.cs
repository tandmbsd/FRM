using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Update_PhieuDieuChinhCongNo
{
    public class Update_PhieuDieuChinhCongNo : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            
            Entity phieudieuchinhcongno1 = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

            if (phieudieuchinhcongno1.Contains("new_chitietbbgiamhuydientich") && phieudieuchinhcongno1.Contains("new_bienbangiamhuydientich") && context.Depth < 2)
            {                
                #region fill from bb giam huy
                Entity en = service.Retrieve("new_chitietbbgiamhuydientich", ((EntityReference)phieudieuchinhcongno1["new_chitietbbgiamhuydientich"]).Id, new ColumnSet(true));                
                Entity BBGiamHuy = service.Retrieve("new_bienbangiamhuydientich", ((EntityReference)phieudieuchinhcongno1["new_bienbangiamhuydientich"]).Id, new ColumnSet(true));                
                Entity chitietHD = service.Retrieve("new_thuadatcanhtac", ((EntityReference)phieudieuchinhcongno1["new_chitiethddtmia"]).Id, new ColumnSet(true));
                
                Entity hopdongdautumia1 = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieudieuchinhcongno1["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_hopdongdautumiaid" }));
                
                if (BBGiamHuy.Contains("statuscode") && ((OptionSetValue)BBGiamHuy["statuscode"]).Value == 100000001)
                {
                    Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)chitietHD["new_chinhsachdautu"]).Id, new ColumnSet(new string[] { "new_dinhmucdautukhonghoanlai", "new_dinhmucdautuhoanlai" }));

                    decimal sotienhoanlaithaydoi = (decimal)en["new_dientichgiam"] * ((Money)chinhsachdautu["new_dinhmucdautuhoanlai"]).Value;
                    decimal sotienkhonghoanlaithaydoi = (decimal)en["new_dientichgiam"] * ((Money)chinhsachdautu["new_dinhmucdautukhonghoanlai"]).Value;
                    decimal sotienconlaihl = (chitietHD.Contains("new_dautuhoanlai") ? ((Money)chitietHD["new_dautuhoanlai"]).Value : 0) - sotienhoanlaithaydoi;
                    decimal sotienconlaikhl = (chitietHD.Contains("new_dautukhonghoanlai") ? ((Money)chitietHD["new_dautukhonghoanlai"]).Value : 0) - sotienkhonghoanlaithaydoi;
                    decimal tongchihoanlai = chitietHD.Contains("new_tongchihoanlai") ? ((Money)chitietHD["new_tongchihoanlai"]).Value : 0;
                    decimal tongchikhonghoanlai = chitietHD.Contains("new_tongchikhonghoanlai") ? ((Money)chitietHD["new_tongchikhonghoanlai"]).Value : 0;

                    Entity phieudieuchinhcongno = new Entity("new_phieudieuchinhcongno");
                    phieudieuchinhcongno.Id = phieudieuchinhcongno1.Id;
                    
                    if (BBGiamHuy.Contains("new_khachhang"))
                    {
                        phieudieuchinhcongno["new_khachhang"] = BBGiamHuy["new_khachhang"];                        
                    }
                    else if (BBGiamHuy.Contains("new_khachhangdoanhnghiep"))
                    {
                        phieudieuchinhcongno["new_khachhangdoanhnghiep"] = BBGiamHuy["new_khachhangdoanhnghiep"];                        
                    }
                    
                    phieudieuchinhcongno["new_tongtienchihlbandau"] = new Money(tongchihoanlai);
                    phieudieuchinhcongno["new_tongtienchikhlbandau"] = new Money(tongchikhonghoanlai);
                    phieudieuchinhcongno["new_dientichthaydoi"] = (decimal)en["new_dientichgiam"];
                    phieudieuchinhcongno["new_dinhmucchinhsachhoanlai"] = chinhsachdautu["new_dinhmucdautuhoanlai"];
                    phieudieuchinhcongno["new_dinhmucchinhsachkhonghoanlai"] = chinhsachdautu["new_dinhmucdautukhonghoanlai"];
                    phieudieuchinhcongno["new_dinhmuchoanlaibandau"] = chitietHD.Contains("new_dautuhoanlai") ? chitietHD["new_dautuhoanlai"] : new Money(0);
                    phieudieuchinhcongno["new_dinhmuckhonghoanlaibandau"] = chitietHD.Contains("new_dautukhonghoanlai") ? chitietHD["new_dautukhonghoanlai"] : new Money(0);
                    phieudieuchinhcongno["new_sotienthaydoi"] = new Money(sotienhoanlaithaydoi);
                    phieudieuchinhcongno["new_sotienthaydoikhl"] = new Money(sotienkhonghoanlaithaydoi);
                    phieudieuchinhcongno["new_sotienconlaihoanlai"] = new Money(sotienconlaihl);
                    phieudieuchinhcongno["new_sotienconlaikhonghoanlai"] = new Money(sotienconlaikhl);
                    phieudieuchinhcongno["new_hopdongdautumia"] = BBGiamHuy["new_hopdongdautumia"];
                    phieudieuchinhcongno["new_chitiethddtmia"] = chitietHD.ToEntityReference();
                    phieudieuchinhcongno["new_phieuphatsinh"] = new OptionSetValue(100000001);
                    phieudieuchinhcongno["new_bienbangiamhuydientich"] = BBGiamHuy.ToEntityReference();
                    phieudieuchinhcongno["new_chitietbbgiamhuydientich"] = en.ToEntityReference();
                    phieudieuchinhcongno["new_phuongthucdieuchinh"] = new OptionSetValue(100000000);                    
                    phieudieuchinhcongno["new_sotiendieuchinh"] = new Money(tongchikhonghoanlai - sotienconlaikhl);
                    service.Update(phieudieuchinhcongno);
                }
                #endregion
            }

            else if (phieudieuchinhcongno1.Contains("nghiemthutrongmia") && phieudieuchinhcongno1.Contains("new_chitietnghiemthutrongmia"))
            {
                #region fill from nttrongmia
                Entity en = service.Retrieve("new_chitietnghiemthutrongmia", ((EntityReference)phieudieuchinhcongno1["new_chitietnghiemthutrongmia"]).Id, new ColumnSet(true));
                Entity nghiemthutrongmia = service.Retrieve("new_nghiemthutrongmia", ((EntityReference)phieudieuchinhcongno1["new_nghiemthutrongmia"]).Id, new ColumnSet(true));
                Entity chitietHD = service.Retrieve("new_thuadatcanhtac", ((EntityReference)phieudieuchinhcongno1["new_chitiethddtmia"]).Id, new ColumnSet(true));
                
                decimal dientichhopdong = (decimal)chitietHD["new_dientichhopdong"];
                decimal dientichnghiemthu = en.Attributes.Contains("new_dientichnghiemthu") ? (decimal)en["new_dientichnghiemthu"] : 0;
                decimal dientichthaydoi = dientichhopdong - dientichnghiemthu;
                dientichthaydoi = Math.Abs(dientichthaydoi);

                Entity hopdongdautumia1 = service.Retrieve("new_hopdongdautumia", ((EntityReference)phieudieuchinhcongno1["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_hopdongdautumiaid" }));

                if (nghiemthutrongmia.Contains("statuscode") && ((OptionSetValue)nghiemthutrongmia["statuscode"]).Value == 100000001)
                {
                    Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)chitietHD["new_chinhsachdautu"]).Id, new ColumnSet(new string[] { "new_dinhmucdautukhonghoanlai", "new_dinhmucdautuhoanlai" }));

                    decimal sotienhoanlaithaydoi = dientichthaydoi* ((Money)chinhsachdautu["new_dinhmucdautuhoanlai"]).Value;
                    decimal sotienkhonghoanlaithaydoi = dientichthaydoi * ((Money)chinhsachdautu["new_dinhmucdautukhonghoanlai"]).Value;
                    decimal sotienconlaihl = (chitietHD.Contains("new_dautuhoanlai") ? ((Money)chitietHD["new_dautuhoanlai"]).Value : 0) - sotienhoanlaithaydoi;
                    decimal sotienconlaikhl = (chitietHD.Contains("new_dautukhonghoanlai") ? ((Money)chitietHD["new_dautukhonghoanlai"]).Value : 0) - sotienkhonghoanlaithaydoi;
                    decimal tongchihoanlai = chitietHD.Contains("new_tongchihoanlai") ? ((Money)chitietHD["new_tongchihoanlai"]).Value : 0;
                    decimal tongchikhonghoanlai = chitietHD.Contains("new_tongchikhonghoanlai") ? ((Money)chitietHD["new_tongchikhonghoanlai"]).Value : 0;

                    Entity phieudieuchinhcongno = new Entity("new_phieudieuchinhcongno");
                    phieudieuchinhcongno.Id = phieudieuchinhcongno1.Id;

                    if (nghiemthutrongmia.Contains("new_khachhang"))
                    {
                        phieudieuchinhcongno["new_khachhang"] = nghiemthutrongmia["new_khachhang"];
                    }
                    else if (nghiemthutrongmia.Contains("new_khachhangdoanhnghiep"))
                    {
                        phieudieuchinhcongno["new_khachhangdoanhnghiep"] = nghiemthutrongmia["new_khachhangdoanhnghiep"];
                    }

                    phieudieuchinhcongno["new_tongtienchihlbandau"] = new Money(tongchihoanlai);
                    phieudieuchinhcongno["new_tongtienchikhlbandau"] = new Money(tongchikhonghoanlai);
                    phieudieuchinhcongno["new_dientichthaydoi"] = dientichthaydoi;
                    phieudieuchinhcongno["new_dinhmucchinhsachhoanlai"] = chinhsachdautu["new_dinhmucdautuhoanlai"];
                    phieudieuchinhcongno["new_dinhmucchinhsachkhonghoanlai"] = chinhsachdautu["new_dinhmucdautukhonghoanlai"];
                    phieudieuchinhcongno["new_dinhmuchoanlaibandau"] = chitietHD.Contains("new_dautuhoanlai") ? chitietHD["new_dautuhoanlai"] : new Money(0);
                    phieudieuchinhcongno["new_dinhmuckhonghoanlaibandau"] = chitietHD.Contains("new_dautukhonghoanlai") ? chitietHD["new_dautukhonghoanlai"] : new Money(0);
                    phieudieuchinhcongno["new_sotienthaydoi"] = new Money(sotienhoanlaithaydoi);
                    phieudieuchinhcongno["new_sotienthaydoikhl"] = new Money(sotienkhonghoanlaithaydoi);
                    phieudieuchinhcongno["new_sotienconlaihoanlai"] = new Money(sotienconlaihl);
                    phieudieuchinhcongno["new_sotienconlaikhonghoanlai"] = new Money(sotienconlaikhl);
                    phieudieuchinhcongno["new_hopdongdautumia"] = nghiemthutrongmia["new_hopdongdautumia"];
                    phieudieuchinhcongno["new_chitiethddtmia"] = chitietHD.ToEntityReference();
                    phieudieuchinhcongno["new_phieuphatsinh"] = new OptionSetValue(100000001);
                    phieudieuchinhcongno["new_nghiemthutrongmia"] = nghiemthutrongmia.ToEntityReference();
                    phieudieuchinhcongno["new_chitietnghiemthutrongmia"] = en.ToEntityReference();
                    phieudieuchinhcongno["new_phuongthucdieuchinh"] = new OptionSetValue(100000000);
                    phieudieuchinhcongno["new_sotiendieuchinh"] = new Money(tongchikhonghoanlai - sotienconlaikhl);
                    service.Update(phieudieuchinhcongno);
                }
                #endregion
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
