using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_DuyetNghiemThuTrongMia
{
    // moi nhat
    public class Plugin_DuyetNghiemThuTrongMia : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("statuscode"))
            {
                if (((OptionSetValue)target["statuscode"]).Value == 100000000)
                {
                    Entity nghiemthutrongmia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    int lannghiemthu = ((OptionSetValue)nghiemthutrongmia["new_lannghiemthu_global"]).Value;
                    trace.Trace("a");
                    List<Entity> dsctNghiemThu = RetrieveMultiRecord(service, "new_chitietnghiemthutrongmia", new ColumnSet(true), "new_nghiemthutrongmia", target.Id);
                    Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)nghiemthutrongmia["new_hopdongtrongmia"]).Id, new ColumnSet(new string[] { "new_masohopdong" }));
                    string mahopdong = hopdongdautumia.Contains("new_masohopdong") ? (string)hopdongdautumia["new_masohopdong"] : "";

                    foreach (Entity en in dsctNghiemThu)
                    {
                        if (en.Contains("new_thuadat"))
                        {
                            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                            q.ColumnSet = new ColumnSet(new string[] { "new_trangthainghiemthu", "statuscode", "new_name", "new_hopdongdautumia" });
                            q.Criteria = new FilterExpression();
                            q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdongdautumia.Id));
                            EntityCollection entc = service.RetrieveMultiple(q);
                            
                            if(entc.Entities.Count == 0)
                                throw new Exception("Chi tiết hợp đồng mía không tồn tại !!!");

                            Entity CTHDDTM = entc.Entities.ToList<Entity>().FirstOrDefault();
                            Entity newCTHDDTM = service.Retrieve(CTHDDTM.LogicalName, CTHDDTM.Id,
                                new ColumnSet(new string[] { "new_trangthainghiemthu", "new_name" }));

                            newCTHDDTM["new_trangthainghiemthu"] = new OptionSetValue(lannghiemthu + 2);
                            service.Update(newCTHDDTM);
                            trace.Trace("Update chi tiet thành công");
                        }
                    }
                    trace.Trace("b");
                    foreach (Entity a in dsctNghiemThu)
                    {
                        string tenkhachhang = "";
                        Entity newCTNT = service.Retrieve("new_chitietnghiemthutrongmia", a.Id, new ColumnSet(true));
                        Entity Giong = service.Retrieve("new_giongmia", ((EntityReference)a["new_giongmia"]).Id, new ColumnSet(true));
                        trace.Trace("1");
                        QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                        q.ColumnSet = new ColumnSet(new string[] { "new_chinhsachdautu", "new_dientichhopdong",
                                "new_ngaytrong", "new_loaigocmia","new_dautuhoanlai","new_dautukhonghoanlai" });
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)a["new_thuadat"]).Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdongdautumia.Id));
                        EntityCollection entc = service.RetrieveMultiple(q);

                        Entity chitietHD = entc.Entities.ToList<Entity>().FirstOrDefault();
                        
                        Entity newCT = service.Retrieve(chitietHD.LogicalName, chitietHD.Id,
                            new ColumnSet(new string[] { "new_tongchihoanlai", "new_dientichhopdong", "new_dientichthucte", "new_giongtrongthucte", "new_tongchikhonghoanlai",
                            "new_ngaythuhoachdukien","new_dinhmucdautuhoanlai_hientai",
                                "new_dinhmucdautukhonghoanlai_hientai","new_ngaytrong","new_name"}));

                        if (!chitietHD.Contains("new_chinhsachdautu"))
                            throw new Exception("Chi tiết HĐ không có chính sách đầu tư !!!");

                        Entity chinhsachdautu = service.Retrieve("new_chinhsachdautu", ((EntityReference)chitietHD["new_chinhsachdautu"]).Id, new ColumnSet(new string[] { "new_dinhmucdautukhonghoanlai", "new_dinhmucdautuhoanlai", "new_name" }));
                        trace.Trace("2");
                        decimal dientichnghiemthu = a.Attributes.Contains("new_dientichnghiemthu") ? (decimal)a["new_dientichnghiemthu"] : 0;
                        decimal dientichhopdong = (decimal)chitietHD["new_dientichhopdong"];
                        decimal dientichthaydoi = dientichhopdong - dientichnghiemthu;
                        dientichthaydoi = Math.Abs(dientichthaydoi);
                        decimal tongchihoanlai = (newCT.Contains("new_tongchihoanlai") ? ((Money)newCT["new_tongchihoanlai"]).Value : 0);
                        decimal tongchikhonghoanlai = (newCT.Contains("new_tongchikhonghoanlai") ? ((Money)newCT["new_tongchikhonghoanlai"]).Value : 0);
                        trace.Trace("3");
                        newCT["new_dientichthucte"] = dientichnghiemthu;
                        newCT["new_giongtrongthucte"] = a.Attributes.Contains("new_giongmia") ? a["new_giongmia"] : null;

                        if (a.Contains("new_ngaytrongxulygoc"))
                        {
                            newCT["new_ngaytrong"] = a.Attributes.Contains("new_ngaytrongxulygoc") ? a["new_ngaytrongxulygoc"] : null;
                            decimal tuoichinmiato = Giong.Contains("new_tuoichinmiato") ? (decimal)Giong["new_tuoichinmiato"] : 0;
                            decimal tuoichinmiagoc = Giong.Contains("new_tuoichinmiagoc") ? (decimal)Giong["new_tuoichinmiagoc"] : 0;

                            if (((OptionSetValue)chitietHD["new_loaigocmia"]).Value == 100000000)
                                newCT["new_ngaythuhoachdukien"] = ((DateTime)a["new_ngaytrongxulygoc"]).AddMonths(0);
                            else
                                newCT["new_ngaythuhoachdukien"] = ((DateTime)a["new_ngaytrongxulygoc"]).AddMonths(0);
                        }

                        if (dientichthaydoi != 0)
                        {
                            Entity phieudieuchinhcongno = new Entity("new_phieudieuchinhcongno");

                            decimal sotienhoanlaithaydoi = dientichthaydoi * ((Money)chinhsachdautu["new_dinhmucdautuhoanlai"]).Value;
                            decimal sotienkhonghoanlaithaydoi = dientichthaydoi * ((Money)chinhsachdautu["new_dinhmucdautukhonghoanlai"]).Value;
                            decimal sotienconlaihl = (chitietHD.Contains("new_dautuhoanlai") ? ((Money)chitietHD["new_dautuhoanlai"]).Value : 0) - sotienhoanlaithaydoi;
                            decimal sotienconlaikhl = (chitietHD.Contains("new_dautukhonghoanlai") ? ((Money)chitietHD["new_dautukhonghoanlai"]).Value : 0) - sotienkhonghoanlaithaydoi;
                            trace.Trace("4");
                            phieudieuchinhcongno["new_sotienconlaihoanlai"] = new Money(sotienconlaihl);
                            phieudieuchinhcongno["new_sotienconlaikhonghoanlai"] = new Money(sotienconlaikhl);
                            phieudieuchinhcongno["new_sotiendieuchinh"] = new Money(0);
                            phieudieuchinhcongno["new_tongtienchihlbandau"] = new Money(tongchihoanlai);
                            phieudieuchinhcongno["new_tongtienchikhlbandau"] = new Money(tongchikhonghoanlai);
                            phieudieuchinhcongno["new_dientichthaydoi"] = dientichthaydoi;
                            phieudieuchinhcongno["new_dinhmucchinhsachhoanlai"] = chinhsachdautu["new_dinhmucdautuhoanlai"];
                            phieudieuchinhcongno["new_dinhmucchinhsachkhonghoanlai"] = chinhsachdautu["new_dinhmucdautukhonghoanlai"];
                            phieudieuchinhcongno["new_dinhmuchoanlaibandau"] = chitietHD.Contains("new_dautuhoanlai") ? chitietHD["new_dautuhoanlai"] : new Money(0);
                            phieudieuchinhcongno["new_dinhmuckhonghoanlaibandau"] = chitietHD.Contains("new_dautukhonghoanlai") ? chitietHD["new_dautukhonghoanlai"] : new Money(0);
                            phieudieuchinhcongno["new_sotienthaydoi"] = new Money(sotienhoanlaithaydoi);
                            phieudieuchinhcongno["new_sotienthaydoikhl"] = new Money(sotienkhonghoanlaithaydoi);
                            phieudieuchinhcongno["new_hopdongdautumia"] = nghiemthutrongmia["new_hopdongtrongmia"];
                            trace.Trace("5");
                            if (nghiemthutrongmia.Contains("new_khachhang"))
                            {
                                phieudieuchinhcongno["new_khachhang"] = nghiemthutrongmia["new_khachhang"];
                                Entity kh = service.Retrieve("contact", ((EntityReference)nghiemthutrongmia["new_khachhang"]).Id, new ColumnSet(new string[] { "new_socmnd" }));
                                tenkhachhang = ((EntityReference)nghiemthutrongmia["new_khachhang"]).Name;
                            }
                            else if (nghiemthutrongmia.Contains("new_khachhangdoanhnghiep"))
                            {
                                phieudieuchinhcongno["new_khachhangdoanhnghiep"] = nghiemthutrongmia["new_khachhangdoanhnghiep"];
                                Entity khdn = service.Retrieve("contact", ((EntityReference)nghiemthutrongmia["new_khachhangdoanhnghiep"]).Id, new ColumnSet(new string[] { "new_sogpkd" }));
                                tenkhachhang = ((EntityReference)nghiemthutrongmia["new_khachhangdoanhnghiep"]).Name;
                            }
                            StringBuilder s = new StringBuilder();
                            s.Append("DC-");

                            if (mahopdong != null)
                            {
                                s.Append(mahopdong + "-");
                            }
                            s.Append(tenkhachhang);
                            phieudieuchinhcongno["new_name"] = "DC-" + mahopdong + "-" + tenkhachhang;
                            phieudieuchinhcongno["new_chitiethddtmia"] = chitietHD.ToEntityReference();
                            phieudieuchinhcongno["new_phieuphatsinh"] = new OptionSetValue(100000000);
                            phieudieuchinhcongno["new_nghiemthutrongmia"] = nghiemthutrongmia.ToEntityReference();
                            phieudieuchinhcongno["new_chitietnghiemthutrongmia"] = a.ToEntityReference();

                            if (sotienconlaihl >= tongchihoanlai)
                            {
                                newCT["new_dinhmucdautuhoanlai_hientai"] = new Money(sotienconlaihl);
                            }

                            if (sotienconlaikhl >= tongchikhonghoanlai)
                            {
                                newCT["new_dinhmucdautukhonghoanlai_hientai"] = new Money(sotienconlaikhl);
                            }
                            else
                            {
                                phieudieuchinhcongno["new_sotiendieuchinh"] = new Money(tongchikhonghoanlai - sotienconlaikhl);
                                service.Create(phieudieuchinhcongno);
                            }
                        }
                        service.Update(newCT);
                        //trace.Trace("có ngay trong xu ly goc: " + ((DateTime)newCT["new_ngaythuhoachdukien"]).ToString() + ((int)Giong["new_tuoichinmiato"]).ToString());
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
