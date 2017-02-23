using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;

namespace Plugin_PhulucHD
{
    public class Plugin_PhulucHD : IPlugin
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

            traceService.Trace(string.Format("Context Depth {0}", context.Depth));
            if (context.Depth > 1)
                return;

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {
                Entity PhulucHD = (Entity)context.InputParameters["Target"];
                Guid entityId = PhulucHD.Id;

                if (PhulucHD.LogicalName == "new_phuluchopdong")
                {
                    traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "UPDATE")
                    {
                        PhulucHD = service.Retrieve("new_phuluchopdong", entityId,
                            new ColumnSet(new string[] { "new_tinhtrangduyet", "new_loaiphuluc", "new_hopdongdautumia", "statuscode",
                                "new_sotientangchomiato", "new_sotientangchomiagoc","new_khachhangcanhan","new_khachhangdoanhnghiep" }));

                        EntityReference HDDTmiaRef = PhulucHD.GetAttributeValue<EntityReference>("new_hopdongdautumia");
                        Guid HDDTmiaId = HDDTmiaRef.Id;
                        Entity HDDTmia = service.Retrieve("new_hopdongdautumia", HDDTmiaId,
                            new ColumnSet(new string[] { "new_vudautu", "new_masohopdong", "new_tram", "new_canbonongvu" }));

                        EntityReference vudautuRef = HDDTmia.GetAttributeValue<EntityReference>("new_vudautu");
                        Guid vuDTId = vudautuRef.Id;
                        Entity VuDT = service.Retrieve("new_vudautu", vuDTId,
                            new ColumnSet(new string[] { "new_name" }));

                        if (PhulucHD.Contains("new_tinhtrangduyet") && PhulucHD.GetAttributeValue<OptionSetValue>("new_tinhtrangduyet").Value.ToString() == "100000005")
                        {
                            #region tang dien tich
                            if (PhulucHD.GetAttributeValue<OptionSetValue>("new_loaiphuluc").Value.ToString() == "100000000")
                            {
                                List<Entity> lstPhuluctangdientich = RetrieveMultiRecord(service, "new_phuluchopdong_tangdientich",
                                    new ColumnSet(true), "new_phuluchopdong", PhulucHD.Id);

                                foreach (Entity en in lstPhuluctangdientich)
                                {
                                    Entity thuadatcanhtac = new Entity("new_thuadatcanhtac");

                                    if (!en.Contains("new_thuadat"))
                                        throw new Exception("Phụ lục HĐ không có thửa đất");
                                    if (!en.Contains("new_chinhsachdautu"))
                                        throw new Exception("Phụ lục HĐ không có chính sách");

                                    Entity td = service.Retrieve("new_thuadat", ((EntityReference)en["new_thuadat"]).Id,
                                        new ColumnSet(new string[] { "new_name", "new_culyvanchuyen",
                                            "new_nhomdat", "new_nhomculy", "new_chusohuuchinhtd","new_chusohuuchinhtdkhdn","new_loaisohuudat" }));
                                    traceService.Trace("0");
                                    thuadatcanhtac["new_culy"] = td.Contains("new_culyvanchuyen") ? td["new_culyvanchuyen"] : "";
                                    thuadatcanhtac["new_nhomdat"] = td.Contains("new_nhomdat") ? td["new_nhomdat"] : "";
                                    thuadatcanhtac["new_nhomculy"] = td.Contains("new_nhomculy") ? td["new_nhomculy"] : "";

                                    if (td.Contains("new_chusohuuchinhtd"))
                                        thuadatcanhtac["new_chusohuuchinhtd"] = td["new_chusohuuchinhtd"];
                                    else
                                        thuadatcanhtac["new_chusohuuchinhtdkhdn"] = td["new_chusohuuchinhtdkhdn"];

                                    traceService.Trace("2");
                                    thuadatcanhtac["new_loaisohuudat"] = td.Contains("new_loaisohuudat") ? td["new_loaisohuudat"] : "";

                                    thuadatcanhtac["new_name"] = "PLHD - " + HDDTmia["new_masohopdong"].ToString() + " - " + td["new_name"].ToString();
                                    thuadatcanhtac["new_thuadat"] = en["new_thuadat"];
                                    thuadatcanhtac["new_chinhsachdautu"] = en["new_chinhsachdautu"];
                                    thuadatcanhtac["new_phuluchopdongid"] = new EntityReference(PhulucHD.LogicalName, PhulucHD.Id);
                                    thuadatcanhtac["new_hopdongdautumia"] = new EntityReference(HDDTmia.LogicalName, HDDTmia.Id);
                                    thuadatcanhtac["new_tram"] = HDDTmia["new_tram"];
                                    thuadatcanhtac["new_canbonongvu"] = HDDTmia["new_canbonongvu"];
                                    thuadatcanhtac["new_dongiadautuhoanlai"] =
                                        en.Contains("new_dongiadautuhoanlai") ? en["new_dongiadautuhoanlai"] : new Money(0);
                                    thuadatcanhtac["new_dongiadautukhonghoanlai"] =
                                        en.Contains("new_dongiadautukhonghoanlai") ? en["new_dongiadautukhonghoanlai"] : new Money(0);

                                    thuadatcanhtac["new_dongiaphanbontoithieu"] =
                                        en.Contains("new_dongiaphanbontoithieu") ? en["new_dongiaphanbontoithieu"] : new Money(0);

                                    thuadatcanhtac["new_dinhmucdautuhoanlai"] =
                                        en.Contains("new_dinhmucdautuhoanlai") ? en["new_dinhmucdautuhoanlai"] : new Money(0);

                                    thuadatcanhtac["new_dinhmucdautukhonghoanlai"] =
                                        en.Contains("new_dinhmucdautukhonghoanlai") ? en["new_dinhmucdautukhonghoanlai"] : new Money(0);

                                    thuadatcanhtac["new_dinhmucdautu"] =
                                        en.Contains("new_dinhmucdautu") ? en["new_dinhmucdautu"] : new Money(0);

                                    thuadatcanhtac["new_dinhmucphanbontoithieu"] =
                                        en.Contains("new_dinhmucphanbontoithieu") ? en["new_dinhmucphanbontoithieu"] : new Money(0);

                                    thuadatcanhtac["new_dongiahopdong"] =
                                        en.Contains("new_dongiahopdong") ? en["new_dongiahopdong"] : new Money(0);

                                    thuadatcanhtac["new_dongiahopdongkhl"] =
                                        en.Contains("new_dongiahopdongkhl") ? en["new_dongiahopdongkhl"] : new Money(0);

                                    thuadatcanhtac["new_dongiaphanbonhd"] =
                                        en.Contains("new_dongiaphanbonhd") ? en["new_dongiaphanbonhd"] : new Money(0);

                                    thuadatcanhtac["new_dautuhoanlai"] =
                                        en.Contains("new_dautuhoanlai") ? en["new_dautuhoanlai"] : new Money(0);

                                    thuadatcanhtac["new_dautukhonghoanlai"] =
                                        en.Contains("new_dautukhonghoanlai") ? en["new_dautukhonghoanlai"] : new Money(0);

                                    thuadatcanhtac["new_conlai_hoanlai"] =
                                        en.Contains("new_dautuhoanlai") ? en["new_dautuhoanlai"] : new Money(0);

                                    thuadatcanhtac["new_conlai_khonghoanlai"] =
                                        en.Contains("new_dautukhonghoanlai") ? en["new_dautukhonghoanlai"] : new Money(0);

                                    thuadatcanhtac["new_conlai_phanbontoithieu"] =
                                        en.Contains("new_dinhmucphanbontoithieu") ? en["new_dinhmucphanbontoithieu"] : new Money(0);

                                    thuadatcanhtac["new_tongchiphidautu"] =
                                        en.Contains("new_tongchiphidautu") ? en["new_tongchiphidautu"] : new Money(0);

                                    if (PhulucHD.Contains("new_khachhangcanhan"))
                                        thuadatcanhtac["new_khachhang"] = PhulucHD["new_khachhangcanhan"];

                                    if (PhulucHD.Contains("new_khachhangdoanhnghiep"))
                                        thuadatcanhtac["new_khachhangdoanhnghiep"] = PhulucHD["new_khachhangdoanhnghiep"];

                                    thuadatcanhtac["new_loaitrong"] = en.Contains("new_loaitrong") ? en["new_loaitrong"] : null;
                                    thuadatcanhtac["new_vutrong"] = en.Contains("new_vutrong") ? en["new_vutrong"] : null;
                                    thuadatcanhtac["new_loaigocmia"] = en.Contains("new_loaigocmia") ? en["new_loaigocmia"] : null;
                                    thuadatcanhtac["new_tuoimia"] = en.Contains("new_tuoimia") ? en["new_tuoimia"] : null;
                                    thuadatcanhtac["new_mucdichsanxuatmia"] = en.Contains("new_mucdichsanxuatmia") ? en["new_mucdichsanxuatmia"] : null;
                                    thuadatcanhtac["new_giongmia"] = en.Contains("new_giongmiadangky") ? en["new_giongmiadangky"] : null;
                                    thuadatcanhtac["new_ngaytrongdukien"] = en.Contains("new_ngaytrongdukien") ? en["new_ngaytrongdukien"] : null;
                                    thuadatcanhtac["new_loaisohuudat"] = en.Contains("new_nguongocdat") ? en["new_nguongocdat"] : null;
                                    thuadatcanhtac["new_dientichhopdong"] = en.Contains("new_dientichhopdong") ? en["new_dientichhopdong"] : null;
                                    thuadatcanhtac["new_dientichconlai"] = en.Contains("new_dientichhopdong") ? en["new_dientichhopdong"] : null;
                                    thuadatcanhtac["statuscode"] = new OptionSetValue(100000000);
                                    thuadatcanhtac["new_trangthainghiemthu"] = new OptionSetValue(100000001);
                                    thuadatcanhtac["new_sonamthuedatconlai"] = en.Contains("new_thoihanthuedatconlai") ? en["new_thoihanthuedatconlai"] : null;

                                    service.Create(thuadatcanhtac);
                                    traceService.Trace("updated tdct");
                                }
                            } // End if Loại phụ lục Tăng diện tích
                            #endregion

                            else if (PhulucHD.GetAttributeValue<OptionSetValue>("new_loaiphuluc").Value.ToString() ==
                                "100000001")
                            {
                                #region tang dinh muc

                                //Loại phụ lục Tăng định mức
                                traceService.Trace("tang dinh muc");

                                List<Entity> lstPhuluctangdinhmuc = RetrieveMultiRecord(service,
                                    "new_phuluchopdong_tangdinhmuc",
                                    new ColumnSet(new string[] { "new_sotientang", "new_thuadat" }), "new_phuluchopdong",
                                    PhulucHD.Id);

                                foreach (Entity en in lstPhuluctangdinhmuc)
                                {
                                    Entity chitietHD = null;
                                    decimal sotientang = en.Contains("new_sotientang")
                                        ? ((Money)en["new_sotientang"]).Value
                                        : 0;
                                    traceService.Trace("1");
                                    QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                                    q.ColumnSet = new ColumnSet(new string[] { "new_dongiahopdong" });
                                    q.Criteria = new FilterExpression();
                                    q.Criteria.AddCondition(new ConditionExpression("new_thuadat",
                                        ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                                    q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia",
                                        ConditionOperator.Equal, HDDTmiaRef.Id));
                                    q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal,
                                        0));
                                    q.Criteria.AddCondition(new ConditionExpression("statuscode",
                                        ConditionOperator.Equal, 100000000));
                                    EntityCollection entc = service.RetrieveMultiple(q);

                                    if (entc.Entities.Count > 0)
                                    {
                                        chitietHD = entc[0];

                                        decimal dongiahl = chitietHD.Contains("new_dongiahopdong")
                                            ? ((Money)chitietHD["new_dongiahopdong"]).Value
                                            : 0;
                                        traceService.Trace("2");
                                        dongiahl += sotientang;

                                        chitietHD["new_dongiahopdong"] = new Money(dongiahl);
                                        service.Update(chitietHD);
                                        traceService.Trace("3");
                                    }
                                }
                                #endregion
                            }
                            // Loại phụ lục Gốc sang Tơ
                            else if (PhulucHD.GetAttributeValue<OptionSetValue>("new_loaiphuluc").Value.ToString() ==
                                     "100000002")
                            {
                                traceService.Trace("start goc sang to");
                                EntityCollection dsPLGocsangTo = FindPLHDGocsangTo(service, PhulucHD);
                                if (dsPLGocsangTo != null && dsPLGocsangTo.Entities.Count > 0)
                                {
                                    foreach (Entity plgocto in dsPLGocsangTo.Entities)
                                    {
                                        Entity plgocsangto = service.Retrieve("new_phuluchopdong_gocsangto", plgocto.Id,
                                            new ColumnSet(true));

                                        EntityReference ctHDDTmiaRef =
                                            plgocsangto.GetAttributeValue<EntityReference>("new_chitiethopdongdautumia");
                                        Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", ctHDDTmiaRef.Id,
                                            new ColumnSet(new string[]
                                            {
                                                "new_vutrong", "new_loaigocmia", "new_mucdichsanxuatmia",
                                                "new_giongmia", "new_thuadat", "createdon", "new_hopdongdautumia",
                                                "new_khachhang",
                                                "new_khachhangdoanhnghiep", "new_thamgiamohinhkhuyennong",
                                                "new_dientichthucte",
                                                "new_tuoimia", "new_dientichhopdong", "new_dinhmucphanbontoithieu",
                                                "new_chinhsachdautu",
                                                "new_luugoc", "new_name", "new_culy", "new_nhomdat", "new_nhomculy",
                                                "new_sonamthuedatconlai",
                                                "new_loaitrong"
                                            }));

                                        DateTime ngaytao = DateTime.Now;

                                        if (ChiTietHD.Contains("createdon"))
                                            ngaytao = ChiTietHD.GetAttributeValue<DateTime>("createdon");

                                        EntityReference thuadatEntityRef = new EntityReference();
                                        Guid thuadatId = new Guid();
                                        Entity thuadatObj = new Entity();

                                        if (ChiTietHD.Attributes.Contains("new_thuadat"))
                                        {
                                            thuadatEntityRef =
                                                ChiTietHD.GetAttributeValue<EntityReference>("new_thuadat");
                                            thuadatId = thuadatEntityRef.Id;
                                            thuadatObj = service.Retrieve("new_thuadat", thuadatId,
                                                new ColumnSet(new string[]
                                                {
                                                    "new_nhomdat", "new_loaisohuudat", "new_vungdialy",
                                                    "new_nhomculy", "new_culyvanchuyen", "new_name"
                                                }));
                                        }

                                        EntityReference giongmiaEntityRef = new EntityReference();
                                        Guid giongmiaId = new Guid();
                                        Entity giongmiaObj = new Entity();

                                        if (ChiTietHD.Attributes.Contains("new_giongmia"))
                                        {
                                            giongmiaEntityRef =
                                                ChiTietHD.GetAttributeValue<EntityReference>("new_giongmia");
                                            giongmiaId = giongmiaEntityRef.Id;
                                            giongmiaObj = service.Retrieve("new_giongmia", giongmiaId,
                                                new ColumnSet(new string[] { "new_nhomgiong", "new_name" }));
                                        }

                                        EntityReference CSDTRef =
                                            plgocsangto.GetAttributeValue<EntityReference>("new_chinhsachdautu");
                                        Guid csdtKQ = CSDTRef.Id;
                                        Entity csdtKQEntity = service.Retrieve("new_chinhsachdautu", csdtKQ,
                                            new ColumnSet(new string[]
                                            {
                                                "new_dinhmucdautuhoanlai", "new_name", "new_loailaisuatcodinhthaydoi",
                                                "new_muclaisuatdautu", "new_cachtinhlai", "new_dinhmucdautukhonghoanlai",
                                                "new_dinhmucphanbontoithieu"
                                            }));

                                        Entity en = new Entity(ChiTietHD.LogicalName);
                                        en.Id = ChiTietHD.Id;

                                        string masothua = "";

                                        //gan nhom cu li , nhom dat , cu li 
                                        if (thuadatObj != null && thuadatObj.Id != Guid.Empty)
                                        {
                                            en["new_culy"] = thuadatObj["new_culyvanchuyen"];
                                            en["new_nhomdat"] = thuadatObj["new_nhomdat"];
                                            en["new_nhomculy"] = thuadatObj["new_nhomculy"];
                                            masothua = " - " + thuadatObj["new_name"].ToString();
                                        }

                                        en["new_name"] = "PLHD - " + HDDTmia["new_masohopdong"].ToString() + masothua;

                                        traceService.Trace("gan nhom cu li , nhom dat, cu li ");


                                        // -------Gan ty le thu hoi von du kien
                                        // Lay nhung tylethuhoivon trong chinh sach dau tu
                                        traceService.Trace("Start find ty le thu hoi von");
                                        EntityCollection collTLTHV = FindtyleTHV(service, csdtKQEntity);

                                        if (collTLTHV != null && collTLTHV.Entities.Count > 0)
                                        {
                                            foreach (Entity TLTHV in collTLTHV.Entities)
                                            {
                                                Entity tlthvdkHDCT = new Entity("new_tylethuhoivondukien");

                                                EntityReference hdctEntityRef = new EntityReference(
                                                    "new_thuadatcanhtac", ChiTietHD.Id);

                                                if (TLTHV.Attributes.Contains("new_phantramtilethuhoi") &&
                                                    TLTHV.Attributes.Contains("new_nam") &&
                                                    csdtKQEntity.Attributes.Contains("new_dinhmucdautuhoanlai"))
                                                {
                                                    string tenTLTHVDK = "Năm " +
                                                                        TLTHV.GetAttributeValue<int>("new_nam")
                                                                            .ToString();
                                                    decimal tyle = (TLTHV.Contains("new_phantramtilethuhoi")
                                                        ? (decimal)TLTHV["new_phantramtilethuhoi"]
                                                        : 0);
                                                    decimal dientichtt = (ChiTietHD.Contains("new_dientichthucte")
                                                        ? (decimal)ChiTietHD["new_dientichthucte"]
                                                        : 0);
                                                    decimal dinhmucDThl =
                                                    (csdtKQEntity.Contains("new_dinhmucdautuhoanlai")
                                                        ? csdtKQEntity.GetAttributeValue<Money>(
                                                            "new_dinhmucdautuhoanlai").Value
                                                        : 0);
                                                    decimal sotien = 0;
                                                    traceService.Trace("1");
                                                    sotien = (dinhmucDThl * dientichtt * tyle) / 100;

                                                    Money sotienM = new Money(sotien);

                                                    tlthvdkHDCT.Attributes.Add("new_name", tenTLTHVDK);
                                                    tlthvdkHDCT.Attributes.Add("new_loaityle",
                                                        new OptionSetValue(100000000));
                                                    tlthvdkHDCT.Attributes.Add("new_chitiethddtmia", hdctEntityRef);
                                                    tlthvdkHDCT.Attributes.Add("new_vudautu", vudautuRef);
                                                    tlthvdkHDCT.Attributes.Add("new_tylephantram", tyle);
                                                    tlthvdkHDCT.Attributes.Add("new_sotienthuhoi", sotienM);

                                                    service.Create(tlthvdkHDCT);
                                                }
                                            }
                                        }
                                        // ------End Gan vao ty le thu hoi von du kien

                                        // Lay thong so vu dau tu
                                        traceService.Trace("Thong so vu dau tu");
                                        EntityCollection collTSVDT = FindthongsoVDT(service, VuDT);

                                        // ------ Gan NHom du lieu Lai suat
                                        if (collTSVDT != null && collTSVDT.Entities.Count > 0)
                                        {
                                            // Loai lai suat
                                            if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                                            {
                                                bool loails =
                                                    csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");

                                                if (loails == false) // ls thay doi
                                                    en["new_loailaisuat"] = new OptionSetValue(100000001);
                                                else // ls co dinh
                                                    en["new_loailaisuat"] = new OptionSetValue(100000000);
                                            }

                                            traceService.Trace("muc lai suat co dinh thay doi");
                                            // Muc lai suat
                                            if (csdtKQEntity.Attributes.Contains("new_loailaisuatcodinhthaydoi"))
                                            {
                                                bool loails =
                                                    csdtKQEntity.GetAttributeValue<bool>("new_loailaisuatcodinhthaydoi");
                                                if (loails == false) // ls thay doi
                                                {
                                                    decimal mucls = (csdtKQEntity.Contains("new_muclaisuatdautu")
                                                        ? (decimal)csdtKQEntity["new_muclaisuatdautu"]
                                                        : 0);
                                                    en["new_laisuat"] = mucls;

                                                }
                                                else // ls co dinh
                                                {
                                                    foreach (Entity TSVDT in collTSVDT.Entities)
                                                    {
                                                        if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value ==
                                                            100000001) //100,000,001 : Loai ls
                                                        {
                                                            if (TSVDT.Attributes.Contains("new_giatri"))
                                                            {
                                                                decimal mucls =
                                                                    TSVDT.GetAttributeValue<decimal>("new_giatri");
                                                                en["new_laisuat"] = mucls;

                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            traceService.Trace("cach tinh lai");
                                            if (csdtKQEntity.Attributes.Contains("new_cachtinhlai"))
                                            {
                                                OptionSetValue cachlinhlai =
                                                    csdtKQEntity.GetAttributeValue<OptionSetValue>("new_cachtinhlai");
                                                en["new_cachtinhlai"] = cachlinhlai;
                                            }

                                            // ------ End nhom du lieu Gan Lai suat

                                            // -------- Gan nhom du lieu  Dinh muc

                                            foreach (Entity TSVDT in collTSVDT.Entities) // Gia mia du kien
                                            {
                                                if (TSVDT.GetAttributeValue<OptionSetValue>("new_loai").Value ==
                                                    100000004) //100,000,004 : Gia mia du kien
                                                {
                                                    if (TSVDT.Attributes.Contains("new_giatien"))
                                                    {
                                                        Money giamiadk = TSVDT.GetAttributeValue<Money>("new_giatien");
                                                        en.Attributes["new_giamiadukien"] = giamiadk;

                                                        break;
                                                    }
                                                }
                                            }
                                        }

                                        decimal dongiabsKHL = 0;
                                        decimal dongiabsHL = 0;
                                        decimal dongiabsPB = 0;
                                        decimal dongiabsTM = 0;

                                        // ------------ Tìm CSDT bổ sung

                                        EntityCollection resultCSDTBS = FindCSDTBS(service, VuDT, ngaytao);

                                        if (resultCSDTBS != null && resultCSDTBS.Entities.Count > 0)
                                        {
                                            foreach (Entity csdtbs in resultCSDTBS.Entities)
                                            {
                                                // NHom khach hang
                                                bool phuhop = true;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        EntityReference nhomkhCSDTBSRef =
                                                            csdtbs.GetAttributeValue<EntityReference>(
                                                                "new_nhomkhachhang");
                                                        Guid khId =
                                                            ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang")
                                                                .Id;
                                                        Entity khObj = service.Retrieve("contact", khId,
                                                            new ColumnSet(new string[] { "fullname", "new_nhomkhachhang" }));

                                                        if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                        {
                                                            Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                                            Guid nhomkhId =
                                                                khObj.GetAttributeValue<EntityReference>(
                                                                    "new_nhomkhachhang").Id;

                                                            if (nhomkhId != nhomkhCSDTBSId)
                                                            {
                                                                phuhop = false;
                                                            }
                                                        }
                                                        else //neu khong co NHomKH trong CTHD
                                                        {
                                                            phuhop = false;
                                                        }

                                                    }
                                                }

                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    if (csdtbs.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        EntityReference nhomkhCSDTBSRef =
                                                            csdtbs.GetAttributeValue<EntityReference>(
                                                                "new_nhomkhachhang");
                                                        Guid khId =
                                                            ChiTietHD.GetAttributeValue<EntityReference>(
                                                                "new_khachhangdoanhnghiep").Id;
                                                        Entity khObj = service.Retrieve("account", khId,
                                                            new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                                        if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                        {
                                                            Guid nhomkhCSDTBSId = nhomkhCSDTBSRef.Id;
                                                            Guid nhomkhId =
                                                                khObj.GetAttributeValue<EntityReference>(
                                                                    "new_nhomkhachhang").Id;

                                                            if (nhomkhId != nhomkhCSDTBSId)
                                                            {
                                                                phuhop = false;
                                                            }
                                                        }
                                                        else //neu khong co NHomKH trong CTHD
                                                        {
                                                            phuhop = false;
                                                        }
                                                    }
                                                }

                                                if (phuhop == false)
                                                    continue;

                                                // Giong mia

                                                phuhop = true;
                                                if (csdtbs.Attributes.Contains("new_giongmia"))
                                                {
                                                    EntityReference giongmiaCSDTBSRef =
                                                        csdtbs.GetAttributeValue<EntityReference>("new_giongmia");
                                                    if (giongmiaEntityRef != null && giongmiaEntityRef.Id != Guid.Empty)
                                                    {
                                                        Guid giongmiaCSDTBSId = giongmiaCSDTBSRef.Id;

                                                        if (giongmiaId != giongmiaCSDTBSId)
                                                        {
                                                            phuhop = false;
                                                        }
                                                    }
                                                    else //neu khong co Giongmia trong CTHD
                                                    {
                                                        phuhop = false;
                                                    }

                                                }
                                                if (phuhop == false)
                                                    continue;

                                                // NHom nang suat

                                                phuhop = true;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                                    {
                                                        EntityReference nhomnangsuatCSDTBSRef =
                                                            csdtbs.GetAttributeValue<EntityReference>("new_nhomnangsuat");
                                                        Guid nhomnangsuatCSDTBSRefId = nhomnangsuatCSDTBSRef.Id;
                                                        Entity nhomnangsuatCSDTBS = service.Retrieve(
                                                            "new_nhomnangsuat", nhomnangsuatCSDTBSRefId,
                                                            new ColumnSet(new string[]
                                                                {"new_nangsuattu", "new_nangsuatden"}));

                                                        Guid khId =
                                                            ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang")
                                                                .Id;
                                                        Entity khObj = service.Retrieve("contact", khId,
                                                            new ColumnSet(new string[]
                                                            {
                                                                "fullname", "new_nhomkhachhang", "new_nangsuatbinhquan"
                                                            }));

                                                        if (khObj.Attributes.Contains("new_nangsuatbinhquan") &&
                                                            nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuattu") &&
                                                            nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuatden"))
                                                        {
                                                            decimal nangsuatbq =
                                                                khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                            decimal nangsuattu =
                                                                nhomnangsuatCSDTBS.GetAttributeValue<decimal>(
                                                                    "new_nangsuattu");
                                                            decimal nangsuatden =
                                                                nhomnangsuatCSDTBS.GetAttributeValue<decimal>(
                                                                    "new_nangsuatden");

                                                            if (
                                                                !((nangsuatbq >= nangsuattu) &&
                                                                  (nangsuatbq <= nangsuatden)))
                                                            {
                                                                phuhop = false;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            phuhop = false;
                                                        }
                                                    }
                                                }

                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    if (csdtbs.Attributes.Contains("new_nhomnangsuat"))
                                                    {
                                                        EntityReference nhomnangsuatCSDTBSRef =
                                                            csdtbs.GetAttributeValue<EntityReference>("new_nhomnangsuat");
                                                        Guid nhomnangsuatCSDTBSRefId = nhomnangsuatCSDTBSRef.Id;
                                                        Entity nhomnangsuatCSDTBS = service.Retrieve(
                                                            "new_nhomnangsuat", nhomnangsuatCSDTBSRefId,
                                                            new ColumnSet(new string[]
                                                                {"new_nangsuattu", "new_nangsuatden"}));

                                                        Guid khId =
                                                            ChiTietHD.GetAttributeValue<EntityReference>(
                                                                "new_khachhangdoanhnghiep").Id;
                                                        Entity khObj = service.Retrieve("account", khId,
                                                            new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                                        if (khObj.Attributes.Contains("new_nangsuatbinhquan") &&
                                                            nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuattu") &&
                                                            nhomnangsuatCSDTBS.Attributes.Contains("new_nangsuatden"))
                                                        {
                                                            decimal nangsuatbq =
                                                                khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                            decimal nangsuattu =
                                                                nhomnangsuatCSDTBS.GetAttributeValue<decimal>(
                                                                    "new_nangsuattu");
                                                            decimal nangsuatden =
                                                                nhomnangsuatCSDTBS.GetAttributeValue<decimal>(
                                                                    "new_nangsuatden");

                                                            if (
                                                                !((nangsuatbq >= nangsuattu) &&
                                                                  (nangsuatbq <= nangsuatden)))
                                                            {
                                                                phuhop = false;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            phuhop = false;
                                                        }
                                                    }
                                                }

                                                if (phuhop == false)
                                                    continue;

                                                // Khuyen khich phat trien

                                                phuhop = true;
                                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service,
                                                    "new_khuyenkhichphattrien", "new_thuadatcanhtac",
                                                    "new_new_chitiethddtmia_new_khuyenkhichpt",
                                                    new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }),
                                                    "new_thuadatcanhtacid", ChiTietHD.Id);

                                                if (csdtbs.Attributes.Contains("new_khuyenkhichphattrien"))
                                                {
                                                    EntityReference kkptCSDTBSRef =
                                                        csdtbs.GetAttributeValue<EntityReference>(
                                                            "new_khuyenkhichphattrien");
                                                    if (dsKKPTHDCT.Entities.Count > 0)
                                                    {
                                                        foreach (Entity kkptHDCT in dsKKPTHDCT.Entities)
                                                        {
                                                            if (kkptHDCT.Id != kkptCSDTBSRef.Id)
                                                            {
                                                                phuhop = false;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        phuhop = false;
                                                    }
                                                }

                                                if (phuhop == false)
                                                    continue;

                                                // Mo hinh khuyen nong

                                                phuhop = true;

                                                if (csdtbs.Attributes.Contains("new_mohinhkhuyennong"))
                                                {
                                                    if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                                    {
                                                        EntityReference mhknEntityRef =
                                                            ChiTietHD.GetAttributeValue<EntityReference>(
                                                                "new_thamgiamohinhkhuyennong");
                                                        EntityReference mhknCSDTBSRef =
                                                            csdtbs.GetAttributeValue<EntityReference>(
                                                                "new_mohinhkhuyennong");

                                                        if (mhknCSDTBSRef.Id != mhknEntityRef.Id)
                                                            phuhop = false;
                                                    }
                                                    else
                                                    {
                                                        phuhop = false;
                                                    }

                                                }
                                                if (phuhop == false)
                                                    continue;

                                                // Nhom cu ly

                                                phuhop = true;

                                                if (csdtbs.Attributes.Contains("new_nhomculy"))
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                                    {
                                                        EntityReference nhomclEntityRef =
                                                            thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy");
                                                        EntityReference nhomclCSDTBSRef =
                                                            csdtbs.GetAttributeValue<EntityReference>("new_nhomculy");
                                                        if (nhomclEntityRef.Id != nhomclCSDTBSRef.Id)
                                                        {
                                                            phuhop = false;
                                                        }
                                                    }
                                                    else
                                                    {
                                                        phuhop = false;
                                                    }
                                                }
                                                if (phuhop == false)
                                                    continue;

                                                //Vung dia ly

                                                phuhop = true;

                                                if (csdtbs.Attributes.Contains("new_vungdialy"))
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                                    {
                                                        EntityReference vungdlEntityRef =
                                                            thuadatObj.GetAttributeValue<EntityReference>(
                                                                "new_vungdialy");
                                                        EntityReference vungdlCSDTBSRef =
                                                            csdtbs.GetAttributeValue<EntityReference>("new_vungdialy");
                                                        if (vungdlEntityRef.Id != vungdlCSDTBSRef.Id)
                                                        {
                                                            phuhop = false;
                                                        }
                                                    }
                                                    else
                                                    {
                                                        phuhop = false;
                                                    }
                                                }
                                                if (phuhop == false)
                                                    continue;
                                                //break;

                                                dongiabsKHL += (csdtbs.Contains("new_sotienbosung_khl")
                                                    ? csdtbs.GetAttributeValue<Money>("new_sotienbosung_khl").Value
                                                    : 0);
                                                dongiabsHL += (csdtbs.Contains("new_sotienbosung")
                                                    ? csdtbs.GetAttributeValue<Money>("new_sotienbosung").Value
                                                    : 0);
                                                dongiabsPB += (csdtbs.Contains("new_bosungphanbon")
                                                    ? csdtbs.GetAttributeValue<Money>("new_bosungphanbon").Value
                                                    : 0);
                                                dongiabsTM += (csdtbs.Contains("new_bosungtienmat")
                                                    ? csdtbs.GetAttributeValue<Money>("new_bosungtienmat").Value
                                                    : 0);

                                                EntityCollection tlthvBScol = FindTLTHVbosung(service, csdtbs);
                                                if (tlthvBScol != null && tlthvBScol.Entities.Count() > 0)
                                                {
                                                    foreach (Entity tlthvbs in tlthvBScol.Entities)
                                                    {
                                                        Entity tlthvdkBS = new Entity("new_tylethuhoivondukien");

                                                        EntityReference hdctEntityRef =
                                                            new EntityReference("new_thuadatcanhtac", ChiTietHD.Id);

                                                        if (tlthvbs.Attributes.Contains("new_phantramtilethuhoi") &&
                                                            tlthvbs.Attributes.Contains("new_nam"))
                                                        {
                                                            string tenTLTHVbs = "Bổ sung năm " +
                                                                                tlthvbs.GetAttributeValue<int>("new_nam")
                                                                                    .ToString();
                                                            decimal tyle = (tlthvbs.Contains("new_phantramtilethuhoi")
                                                                ? (decimal)tlthvbs["new_phantramtilethuhoi"]
                                                                : 0);
                                                            decimal dientichtt =
                                                            (ChiTietHD.Contains("new_dientichthucte")
                                                                ? (decimal)ChiTietHD["new_dientichthucte"]
                                                                : 0);
                                                            decimal dinhmucDThl = dongiabsHL + dongiabsPB + dongiabsTM;
                                                            decimal sotien = 0;

                                                            sotien = (dinhmucDThl * dientichtt * tyle) / 100;

                                                            Money sotienM = new Money(sotien);

                                                            tlthvdkBS.Attributes.Add("new_name", tenTLTHVbs);
                                                            tlthvdkBS.Attributes.Add("new_loaityle",
                                                                new OptionSetValue(100000000));
                                                            tlthvdkBS.Attributes.Add("new_chitiethddtmia", hdctEntityRef);
                                                            tlthvdkBS.Attributes.Add("new_vudautu", vudautuRef);
                                                            tlthvdkBS.Attributes.Add("new_tylephantram", tyle);
                                                            tlthvdkBS.Attributes.Add("new_sotienthuhoi", sotienM);

                                                            service.Create(tlthvdkBS);
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        // ----------------- DINH MUC KHONG HOAN LAI

                                        decimal dientichhd = (ChiTietHD.Contains("new_dientichhopdong")
                                            ? (decimal)ChiTietHD["new_dientichhopdong"]
                                            : 0);
                                        decimal dongiaDTKHL = (csdtKQEntity.Contains("new_dinhmucdautukhonghoanlai")
                                            ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautukhonghoanlai")
                                                .Value
                                            : 0);
                                        decimal dongiaDTHL = (csdtKQEntity.Contains("new_dinhmucdautuhoanlai")
                                            ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucdautuhoanlai").Value
                                            : 0);
                                        decimal dongiaPhanbon = (csdtKQEntity.Contains("new_dinhmucphanbontoithieu")
                                            ? csdtKQEntity.GetAttributeValue<Money>("new_dinhmucphanbontoithieu").Value
                                            : 0);

                                        dongiaDTKHL += dongiabsKHL;
                                        Money MdongiaDTKHL = new Money(dongiaDTKHL);

                                        decimal dinhmucDTKHL = dongiaDTKHL * dientichhd;
                                        Money MdinhmucDTKHL = new Money(dinhmucDTKHL);

                                        //traceService.Trace("truoc cap nhat " + tongDMDTKHL);

                                        en.Attributes.Add("new_dongiadautukhonghoanlai", MdongiaDTKHL);
                                        en.Attributes.Add("new_dinhmucdautukhonghoanlai", MdinhmucDTKHL);

                                        // ----------END------- DINH MUC KHONG HOAN LAI

                                        // ----------------- DINH MUC DAU TU HOAN LAI

                                        dongiaDTHL = dongiaDTHL - dongiaPhanbon + dongiabsHL + dongiabsPB + dongiabsTM;

                                        decimal dinhmucDTHL = dongiaDTHL * dientichhd;

                                        Money MdongiaDT = new Money(dongiaDTHL);
                                        Money MdinhmucDT = new Money(dinhmucDTHL);

                                        en.Attributes.Add("new_dongiadautuhoanlai", MdongiaDT);
                                        en.Attributes.Add("new_dinhmucdautuhoanlai", MdinhmucDT);

                                        // -------------------- DINH MUC PHAN BON

                                        decimal tongDMPB = 0;

                                        if (!HDDTmia.Contains("new_chinhantienmat") ||
                                            (HDDTmia.Contains("new_chinhantienmat") &&
                                             (bool)HDDTmia["new_chinhantienmat"] == false))
                                        {
                                            Money MdongiaPhanbon = new Money(dongiaPhanbon);
                                            tongDMPB = dongiaPhanbon * dientichhd;
                                            Money MtongDMDTKHL = new Money(tongDMPB);

                                            en.Attributes.Add("new_dongiaphanbontoithieu", MdongiaPhanbon);
                                            en.Attributes.Add("new_dinhmucphanbontoithieu", MtongDMDTKHL);
                                        }

                                        // --------END--------- DINH MUC PHAN BON

                                        // -------------------- DINH MUC DAU TU

                                        decimal tongDM = dinhmucDTHL + dinhmucDTKHL + tongDMPB;
                                        Money MtongDM = new Money(tongDM);

                                        en.Attributes.Add("new_dinhmucdautu", MtongDM);

                                        // --------END--------- DINH MUC DAU TU

                                        // -------- End nhom du lieu  Gan Dinh muc

                                        // Gán loại gốc mía là mía tơ
                                        en["new_loaigocmia"] = new OptionSetValue(100000000);
                                        // Gán CSDT
                                        en["new_chinhsachdautu"] = CSDTRef;
                                        traceService.Trace("2");
                                        // ------------- Load Đầu tư
                                        en["new_sonamthuedatconlai"] = plgocsangto.Contains("new_thoihanthuedatconlai")
                                            ? (int)plgocsangto["new_thoihanthuedatconlai"]
                                            : 0;
                                        en["new_loaitrong"] = plgocsangto.Contains("new_loaitrong")
                                            ? plgocsangto["new_loaitrong"]
                                            : null;
                                        en["new_mucdichsanxuatmia"] = plgocsangto.Contains("new_mucdichsanxuatmia")
                                            ? plgocsangto["new_mucdichsanxuatmia"]
                                            : null;
                                        en["new_giongmia"] = plgocsangto.Contains("new_giongmiadangky")
                                            ? plgocsangto["new_giongmiadangky"]
                                            : null;
                                        en["new_dongiahopdong"] = plgocsangto.Contains("new_dongiahopdong")
                                            ? (Money)plgocsangto["new_dongiahopdong"]
                                            : new Money(0);
                                        traceService.Trace("3");
                                        en["new_dongiahopdongkhl"] = plgocsangto.Contains("new_dongiahopdongkhl")
                                            ? (Money)plgocsangto["new_dongiahopdongkhl"]
                                            : new Money(0);
                                        traceService.Trace("4");
                                        en["new_dongiaphanbonhd"] = plgocsangto.Contains("new_dongiaphanbonhd")
                                            ? (Money)plgocsangto["new_dongiaphanbonhd"]
                                            : new Money(0);
                                        traceService.Trace("5");
                                        en["new_dautuhoanlai"] = plgocsangto.Contains("new_dautuhoanlai")
                                            ? (Money)plgocsangto["new_dautuhoanlai"]
                                            : new Money(0);
                                        traceService.Trace("6");
                                        en["new_dautukhonghoanlai"] = plgocsangto.Contains("new_dautukhonghoanlai")
                                            ? (Money)plgocsangto["new_dautukhonghoanlai"]
                                            : new Money(0);
                                        traceService.Trace("7");
                                        en["new_tongchiphidautu"] = plgocsangto.Contains("new_tongchiphidautu")
                                            ? (Money)plgocsangto["new_tongchiphidautu"]
                                            : new Money(0);
                                        en["new_sotienphanbontoithieu"] =
                                            (Money)plgocsangto["new_sotienphanbontoithieu"];

                                        // --- End ----  Load Đầu tư
                                        service.Update(en);

                                        EntityReferenceCollection OldlistCSDTRef = new EntityReferenceCollection();
                                        EntityCollection OldlistCSDT = RetrieveNNRecord(service, "new_chinhsachdautu",
                                            "new_thuadatcanhtac", "new_new_chitiethddtmia_new_chinhsachdautu",
                                            new ColumnSet(new string[] { "new_chinhsachdautuid" }), "new_thuadatcanhtacid",
                                            ChiTietHD.Id);
                                        foreach (Entity oldCSDT in OldlistCSDT.Entities)
                                        {
                                            OldlistCSDTRef.Add(oldCSDT.ToEntityReference());
                                        }

                                        service.Disassociate("new_thuadatcanhtac", ChiTietHD.Id,
                                            new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"),
                                            OldlistCSDTRef);

                                        EntityReferenceCollection listCSDT = new EntityReferenceCollection();
                                        listCSDT.Add(csdtKQEntity.ToEntityReference());

                                        //-------- Tìm CSDT thâm canh --------------------

                                        EntityCollection csdtThamcanhCol = FindCSDTthamcanh(service, ChiTietHD);
                                        Entity mCSDTthamcanh = null;

                                        if (csdtThamcanhCol != null && csdtThamcanhCol.Entities.Count > 0)
                                        {
                                            foreach (Entity a in csdtThamcanhCol.Entities)
                                            {
                                                if (a.Contains("new_vutrong_vl")) // Vu trong
                                                {
                                                    if (ChiTietHD.Contains("new_vutrong"))
                                                    {
                                                        if (
                                                            a["new_vutrong_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_vutrong"]).Value
                                                                        .ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_loaigocmia_vl")) // Loai goc mia
                                                {
                                                    if (ChiTietHD.Contains("new_loaigocmia"))
                                                    {
                                                        if (a["new_loaigocmia_vl"].ToString().IndexOf("100000000") == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }


                                                if (a.Contains("new_mucdichsanxuatmia_vl")) // Muc dich san xuat mia
                                                {
                                                    if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                                    {
                                                        if (
                                                            a["new_mucdichsanxuatmia_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_nhomdat_vl")) // Nhom dat
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                                    {
                                                        if (
                                                            a["new_nhomdat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_nhomdat"]).Value
                                                                        .ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_loaisohuudat_vl")) // Loai chu so huu
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                                    {
                                                        if (
                                                            a["new_loaisohuudat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_loaisohuudat"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_nhomgiongmia_vl")) // Nhom giong mia
                                                {
                                                    if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                                    {
                                                        if (
                                                            a["new_nhomgiongmia_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)giongmiaObj["new_nhomgiong"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                // Nhom khach hang
                                                bool co = false;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[]
                                                            {"fullname", "new_nhomkhachhang", "new_nangsuatbinhquan"}));

                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {
                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }
                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {
                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (co == false)
                                                    continue;

                                                //Vung dia ly
                                                co = false;

                                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung",
                                                    new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid",
                                                    a.Id);

                                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                                {
                                                    Guid vungdlId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy")
                                                            .Id;
                                                    Entity vungDL = service.Retrieve("new_vung", vungdlId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                                        {
                                                            if (vungDL.Id == vungDL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co VungDiaLy trong CTHD
                                                {
                                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // Giong mia
                                                co = false;
                                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia",
                                                    new ColumnSet(new string[] { "new_giongmiaid" }),
                                                    "new_chinhsachdautuid", a.Id);
                                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                                {
                                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                                    {
                                                        if (giongmiaObj.Id == giongmia.Id)
                                                        {
                                                            co = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                else
                                                    co = true;

                                                if (co == false)
                                                    continue;

                                                // Khuyen khich phat trien
                                                co = false;
                                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service,
                                                    "new_khuyenkhichphattrien", "new_thuadatcanhtac",
                                                    "new_new_chitiethddtmia_new_khuyenkhichpt",
                                                    new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }),
                                                    "new_thuadatcanhtacid", ChiTietHD.Id);
                                                EntityCollection dsKKPTCSDT = RetrieveNNRecord(service,
                                                    "new_khuyenkhichphattrien", "new_chinhsachdautu",
                                                    "new_new_chinhsachdautu_new_khuyenkhichphatt",
                                                    new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }),
                                                    "new_chinhsachdautuid", a.Id);

                                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                                {
                                                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                                    {
                                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                                        {
                                                            foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                                            {
                                                                //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                                                if (kkpt1.Id == kkpt2.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                            if (co)
                                                                break; //thoat vong for thu 1
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co KKPT trong CTHD
                                                {
                                                    if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // Nhom cu ly
                                                co = false;

                                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy",
                                                    new ColumnSet(new string[] { "new_nhomculyid" }),
                                                    "new_chinhsachdautuid", a.Id);
                                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                                {
                                                    Guid nhomclId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                                        {
                                                            if (nhomCL.Id == nhomCL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co NHomCL trong CTHD
                                                {
                                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // Mo hinh khuyen nong
                                                co = false;

                                                EntityCollection dsMHKN = RetrieveNNRecord(service,
                                                    "new_mohinhkhuyennong", "new_chinhsachdautu",
                                                    "new_new_chinhsachdautu_new_mohinhkhuyennong",
                                                    new ColumnSet(new string[] { "new_mohinhkhuyennongid" }),
                                                    "new_chinhsachdautuid", a.Id);

                                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                                {
                                                    Guid mhknId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_thamgiamohinhkhuyennong").Id;
                                                    Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                                    {
                                                        foreach (Entity mhkn1 in dsMHKN.Entities)
                                                        {
                                                            if (mhkn.Id == mhkn1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co MNKH trong CTHD
                                                {
                                                    if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // NHom nang suat
                                                co = false;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));
                                                    EntityCollection dsNhomNS = RetrieveNNRecord(service,
                                                        "new_nhomnangsuat", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomnangsuat",
                                                        new ColumnSet(new string[]
                                                            {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                decimal nangsuattu =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                                decimal nangsuatden =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                                if ((nangsuatbq >= nangsuattu) &&
                                                                    (nangsuatbq <= nangsuatden))
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }
                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));
                                                    EntityCollection dsNhomNS = RetrieveNNRecord(service,
                                                        "new_nhomnangsuat", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomnangsuat",
                                                        new ColumnSet(new string[]
                                                            {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                decimal nangsuattu =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                                decimal nangsuatden =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                                if ((nangsuatbq >= nangsuattu) &&
                                                                    (nangsuatbq <= nangsuatden))
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (co == false)
                                                    continue;

                                                mCSDTthamcanh = a;
                                                break;
                                            }
                                            if (mCSDTthamcanh != null && mCSDTthamcanh.Id != Guid.Empty)
                                                listCSDT.Add(mCSDTthamcanh.ToEntityReference());
                                            //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTthamcanh.ToEntityReference() });
                                        }
                                        //----END---- Tìm CSDT thâm canh --------------------

                                        //-------- Tìm CSDT tưới mía --------------------

                                        EntityCollection csdtTuoimiaCol = FindCSDTtuoi(service, ChiTietHD);
                                        Entity mCSDTtuoimia = null;

                                        if (csdtTuoimiaCol != null && csdtTuoimiaCol.Entities.Count > 0)
                                        {
                                            foreach (Entity a in csdtTuoimiaCol.Entities)
                                            {
                                                if (a.Contains("new_mucdichtuoi_vl")) // Muc dich tuoi
                                                {
                                                    if (ChiTietHD.Attributes.Contains("new_mucdichtuoi"))
                                                    {
                                                        if (
                                                            a["new_mucdichtuoi_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_mucdichtuoi"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_phuongphaptuoi_vl")) // Phuong phap tuoi
                                                {
                                                    if (ChiTietHD.Attributes.Contains("new_phuongphaptuoi"))
                                                    {
                                                        if (
                                                            a["new_phuongphaptuoi_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_phuongphaptuoi"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_nhomdat_vl")) // Nhom dat
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                                    {
                                                        if (
                                                            a["new_nhomdat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_nhomdat"]).Value
                                                                        .ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_loaisohuudat_vl")) // Loai chu so huu
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                                    {
                                                        if (
                                                            a["new_loaisohuudat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_loaisohuudat"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                traceService.Trace("vi trí Nhom KH");
                                                // NHom khach hang
                                                bool co = false;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[]
                                                            {"fullname", "new_nhomkhachhang", "new_nangsuatbinhquan"}));

                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {
                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {
                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }
                                                if (co == false)
                                                    continue;

                                                traceService.Trace("vi trí truoc Vung dia ly");
                                                //Vung dia ly
                                                co = false;

                                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung",
                                                    new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid",
                                                    a.Id);

                                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                                {
                                                    Guid vungdlId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy")
                                                            .Id;
                                                    Entity vungDL = service.Retrieve("new_vung", vungdlId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                                        {
                                                            if (vungDL.Id == vungDL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co VungDiaLy trong CTHD
                                                {
                                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                                        co = true;
                                                }

                                                if (co == false)
                                                    continue;

                                                traceService.Trace("vi trí truoc Nhom cu ly");
                                                // Nhom cu ly
                                                co = false;

                                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy",
                                                    new ColumnSet(new string[] { "new_nhomculyid" }),
                                                    "new_chinhsachdautuid", a.Id);
                                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                                {
                                                    Guid nhomclId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                                        {
                                                            if (nhomCL.Id == nhomCL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                else //neu khong co NHomCL trong CTHD
                                                {
                                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                traceService.Trace("vi trí truoc NHom nang suat");
                                                // NHom nang suat
                                                co = false;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));
                                                    EntityCollection dsNhomNS = RetrieveNNRecord(service,
                                                        "new_nhomnangsuat", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomnangsuat",
                                                        new ColumnSet(new string[]
                                                            {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                decimal nangsuattu =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                                decimal nangsuatden =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                                if ((nangsuatbq >= nangsuattu) &&
                                                                    (nangsuatbq <= nangsuatden))
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));
                                                    EntityCollection dsNhomNS = RetrieveNNRecord(service,
                                                        "new_nhomnangsuat", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomnangsuat",
                                                        new ColumnSet(new string[]
                                                            {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                decimal nangsuattu =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                                decimal nangsuatden =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                                if ((nangsuatbq >= nangsuattu) &&
                                                                    (nangsuatbq <= nangsuatden))
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }

                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (co == false)
                                                    continue;

                                                mCSDTtuoimia = a;
                                                break;
                                            }
                                            if (mCSDTtuoimia != null && mCSDTtuoimia.Id != Guid.Empty)
                                                listCSDT.Add(mCSDTtuoimia.ToEntityReference());
                                            //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTtuoimia.ToEntityReference() });
                                        }
                                        //----END---- Tìm CSDT tưới mía --------------------

                                        //-------- Tìm CSDT bóc lá mía --------------------

                                        traceService.Trace("vi trí bat dau Tìm CSDT bóc lá mía");
                                        EntityCollection csdtBocLamiaCol = FindCSDTbocla(service, ChiTietHD);
                                        Entity mCSDTbocla = null;

                                        if (csdtBocLamiaCol != null && csdtBocLamiaCol.Entities.Count > 0)
                                        {
                                            foreach (Entity a in csdtBocLamiaCol.Entities)
                                            {
                                                if (a.Contains("new_vutrong_vl")) // Vu trong
                                                {
                                                    if (ChiTietHD.Contains("new_vutrong"))
                                                    {
                                                        if (
                                                            a["new_vutrong_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_vutrong"]).Value
                                                                        .ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_loaigocmia_vl")) // Loai goc mia
                                                {
                                                    if (ChiTietHD.Contains("new_loaigocmia"))
                                                    {
                                                        if (a["new_loaigocmia_vl"].ToString().IndexOf("100000000") == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_mucdichsanxuatmia_vl")) // Muc dich san xuat mia
                                                {
                                                    if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                                    {
                                                        if (
                                                            a["new_mucdichsanxuatmia_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_nhomdat_vl")) // Nhom dat
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                                    {
                                                        if (
                                                            a["new_nhomdat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_nhomdat"]).Value
                                                                        .ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_loaisohuudat_vl")) // Loai chu so huu
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                                    {
                                                        if (
                                                            a["new_loaisohuudat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_loaisohuudat"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_nhomgiongmia_vl")) // Nhom giong mia
                                                {
                                                    if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                                    {
                                                        if (
                                                            a["new_nhomgiongmia_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)giongmiaObj["new_nhomgiong"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                // NHom khach hang
                                                bool co = false;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[]
                                                            {"fullname", "new_nhomkhachhang", "new_nangsuatbinhquan"}));

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {
                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {
                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (co == false)
                                                    continue;

                                                //Vung dia ly
                                                co = false;

                                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung",
                                                    new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid",
                                                    a.Id);

                                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                                {
                                                    Guid vungdlId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy")
                                                            .Id;
                                                    Entity vungDL = service.Retrieve("new_vung", vungdlId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                                        {
                                                            if (vungDL.Id == vungDL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co VungDiaLy trong CTHD
                                                {
                                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // Giong mia
                                                co = false;

                                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia",
                                                    new ColumnSet(new string[] { "new_giongmiaid" }),
                                                    "new_chinhsachdautuid", a.Id);
                                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                                {
                                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                                    {
                                                        if (giongmiaObj.Id == giongmia.Id)
                                                        {
                                                            co = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                else
                                                    co = true;

                                                if (co == false)
                                                    continue;

                                                // Khuyen khich phat trien
                                                co = false;
                                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service,
                                                    "new_khuyenkhichphattrien", "new_thuadatcanhtac",
                                                    "new_new_chitiethddtmia_new_khuyenkhichpt",
                                                    new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }),
                                                    "new_thuadatcanhtacid", ChiTietHD.Id);
                                                EntityCollection dsKKPTCSDT = RetrieveNNRecord(service,
                                                    "new_khuyenkhichphattrien", "new_chinhsachdautu",
                                                    "new_new_chinhsachdautu_new_khuyenkhichphatt",
                                                    new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }),
                                                    "new_chinhsachdautuid", a.Id);

                                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                                {
                                                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                                    {
                                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                                        {
                                                            foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                                            {
                                                                //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                                                if (kkpt1.Id == kkpt2.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                            if (co)
                                                                //thoat vong for thu 1
                                                                break;
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co KKPT trong CTHD
                                                {
                                                    if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // Nhom cu ly
                                                co = false;

                                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy",
                                                    new ColumnSet(new string[] { "new_nhomculyid" }),
                                                    "new_chinhsachdautuid", a.Id);
                                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                                {
                                                    Guid nhomclId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                                        {
                                                            if (nhomCL.Id == nhomCL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co NHomCL trong CTHD
                                                {
                                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // Mo hinh khuyen nong
                                                co = false;

                                                EntityCollection dsMHKN = RetrieveNNRecord(service,
                                                    "new_mohinhkhuyennong", "new_chinhsachdautu",
                                                    "new_new_chinhsachdautu_new_mohinhkhuyennong",
                                                    new ColumnSet(new string[] { "new_mohinhkhuyennongid" }),
                                                    "new_chinhsachdautuid", a.Id);

                                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                                {
                                                    Guid mhknId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_thamgiamohinhkhuyennong").Id;
                                                    Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                                    {
                                                        //List<Entity> ldsMHKN = dsMHKN.Entities.ToList<Entity>();

                                                        //foreach (Entity mhkn1 in ldsMHKN)
                                                        foreach (Entity mhkn1 in dsMHKN.Entities)
                                                        {
                                                            if (mhkn.Id == mhkn1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                        co = true;
                                                }
                                                else //neu khong co MNKH trong CTHD
                                                {
                                                    if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                                        co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // NHom nang suat
                                                co = false;
                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                                    EntityCollection dsNhomNS = RetrieveNNRecord(service,
                                                        "new_nhomnangsuat", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomnangsuat",
                                                        new ColumnSet(new string[]
                                                            {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                decimal nangsuattu =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                                decimal nangsuatden =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                                if ((nangsuatbq >= nangsuattu) &&
                                                                    (nangsuatbq <= nangsuatden))
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }
                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                                    EntityCollection dsNhomNS = RetrieveNNRecord(service,
                                                        "new_nhomnangsuat", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomnangsuat",
                                                        new ColumnSet(new string[]
                                                            {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                                        "new_chinhsachdautuid", a.Id);

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                decimal nangsuattu =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuattu");
                                                                decimal nangsuatden =
                                                                    mhkn1.GetAttributeValue<decimal>("new_nangsuatden");

                                                                if ((nangsuatbq >= nangsuattu) &&
                                                                    (nangsuatbq <= nangsuatden))
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                            co = true;
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                            co = true;
                                                    }
                                                }

                                                if (co == false)
                                                    continue;

                                                mCSDTbocla = a;
                                                break;
                                            }
                                            if (mCSDTbocla != null && mCSDTbocla.Id != Guid.Empty)
                                                listCSDT.Add(mCSDTbocla.ToEntityReference());
                                            //service.Associate("new_thuadatcanhtac", ChiTietHD.Id, new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), new EntityReferenceCollection() { mCSDTbocla.ToEntityReference() }); 
                                        }
                                        //----END---- Tìm CSDT bóc lá mía --------------------

                                        //----------- Tìm CSDT ứng --------------------

                                        EntityCollection csdtUngCol = FindCSDTung(service, ChiTietHD);
                                        Entity mCSDTung = null;

                                        if (csdtUngCol != null && csdtUngCol.Entities.Count > 0)
                                        {
                                            foreach (Entity a in csdtUngCol.Entities)
                                            {
                                                if (a.Contains("new_vutrong_vl")) // Vu trong
                                                {
                                                    if (ChiTietHD.Contains("new_vutrong"))
                                                    {
                                                        if (
                                                            a["new_vutrong_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_vutrong"]).Value
                                                                        .ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_loaigocmia_vl")) // Loai goc mia
                                                {
                                                    if (ChiTietHD.Contains("new_loaigocmia"))
                                                    {
                                                        if (a["new_loaigocmia_vl"].ToString().IndexOf("100000000") == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_mucdichsanxuatmia_vl")) // Muc dich san xuat mia
                                                {
                                                    if (ChiTietHD.Contains("new_mucdichsanxuatmia"))
                                                    {
                                                        if (
                                                            a["new_mucdichsanxuatmia_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)ChiTietHD["new_mucdichsanxuatmia"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                        continue;
                                                }

                                                if (a.Contains("new_nhomdat_vl")) // Nhom dat
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_nhomdat"))
                                                    {
                                                        if (
                                                            a["new_nhomdat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_nhomdat"]).Value
                                                                        .ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                    {
                                                        continue;
                                                    }
                                                }

                                                if (a.Contains("new_loaisohuudat_vl")) // Loai chu so huu
                                                {
                                                    if (thuadatObj.Attributes.Contains("new_loaisohuudat"))
                                                    {
                                                        if (
                                                            a["new_loaisohuudat_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)thuadatObj["new_loaisohuudat"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                    {
                                                        continue;
                                                    }
                                                }

                                                if (a.Contains("new_nhomgiongmia_vl")) // Nhom giong mia
                                                {
                                                    if (giongmiaObj.Attributes.Contains("new_nhomgiong"))
                                                    {
                                                        if (
                                                            a["new_nhomgiongmia_vl"].ToString()
                                                                .IndexOf(
                                                                    ((OptionSetValue)giongmiaObj["new_nhomgiong"])
                                                                        .Value.ToString()) == -1)
                                                            continue;
                                                    }
                                                    else
                                                    {
                                                        continue;
                                                    }
                                                }

                                                // NHom khach hang
                                                bool co = false;

                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[]
                                                            {"fullname", "new_nhomkhachhang", "new_nangsuatbinhquan"}));

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {

                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                }

                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    EntityCollection dsNhomKH = RetrieveNNRecord(service,
                                                        "new_nhomkhachhang", "new_chinhsachdautu",
                                                        "new_new_chinhsachdautu_new_nhomkhachhang",
                                                        new ColumnSet(new string[] { "new_nhomkhachhangid" }),
                                                        "new_chinhsachdautuid", a.Id);

                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nhomkhachhang" }));

                                                    if (khObj.Attributes.Contains("new_nhomkhachhang"))
                                                    {
                                                        Guid nhomkhId =
                                                            khObj.GetAttributeValue<EntityReference>("new_nhomkhachhang")
                                                                .Id;
                                                        Entity nhomKHHDCT = service.Retrieve("new_nhomkhachhang",
                                                            nhomkhId, new ColumnSet(new string[] { "new_name" }));
                                                        if (dsNhomKH != null && dsNhomKH.Entities.Count > 0)
                                                        {
                                                            foreach (Entity nhomKH in dsNhomKH.Entities)
                                                            {
                                                                if (nhomKHHDCT.Id == nhomKH.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                    else //neu khong co NHomKH trong CTHD
                                                    {

                                                        if (dsNhomKH == null || dsNhomKH.Entities.Count() == 0)
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                }
                                                if (co == false)
                                                    continue;

                                                //Vung dia ly
                                                co = false;

                                                EntityCollection dsVungDL = RetrieveNNRecord(service, "new_vung",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_vung",
                                                    new ColumnSet(new string[] { "new_vungid" }), "new_chinhsachdautuid",
                                                    a.Id);

                                                if (thuadatObj.Attributes.Contains("new_vungdialy"))
                                                {
                                                    Guid vungdlId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_vungdialy")
                                                            .Id;
                                                    Entity vungDL = service.Retrieve("new_vung", vungdlId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsVungDL != null && dsVungDL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity vungDL1 in dsVungDL.Entities)
                                                        {
                                                            if (vungDL.Id == vungDL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                else //neu khong co VungDiaLy trong CTHD
                                                {

                                                    if (dsVungDL == null || dsVungDL.Entities.Count() == 0)
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                if (co == false)
                                                    continue;

                                                // Giong mia
                                                co = false;

                                                EntityCollection dsGiongmia = RetrieveNNRecord(service, "new_giongmia",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_giongmia",
                                                    new ColumnSet(new string[] { "new_giongmiaid" }),
                                                    "new_chinhsachdautuid", a.Id);
                                                if (dsGiongmia != null && dsGiongmia.Entities.Count > 0)
                                                {
                                                    foreach (Entity giongmia in dsGiongmia.Entities)
                                                    {
                                                        if (giongmiaObj.Id == giongmia.Id)
                                                        {
                                                            co = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    co = true;
                                                }
                                                if (co == false)
                                                    continue;

                                                // Khuyen khich phat trien
                                                co = false;
                                                EntityCollection dsKKPTHDCT = RetrieveNNRecord(service,
                                                    "new_khuyenkhichphattrien", "new_thuadatcanhtac",
                                                    "new_new_chitiethddtmia_new_khuyenkhichpt",
                                                    new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }),
                                                    "new_thuadatcanhtacid", ChiTietHD.Id);
                                                EntityCollection dsKKPTCSDT = RetrieveNNRecord(service,
                                                    "new_khuyenkhichphattrien", "new_chinhsachdautu",
                                                    "new_new_chinhsachdautu_new_khuyenkhichphatt",
                                                    new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }),
                                                    "new_chinhsachdautuid", a.Id);

                                                if (dsKKPTHDCT != null && dsKKPTHDCT.Entities.Count > 0)
                                                {
                                                    if (dsKKPTCSDT != null && dsKKPTCSDT.Entities.Count > 0)
                                                    {
                                                        foreach (Entity kkpt1 in dsKKPTHDCT.Entities)
                                                        {
                                                            foreach (Entity kkpt2 in dsKKPTCSDT.Entities)
                                                            {
                                                                //neu tim thay kkpt1 nam trong danh sach dsKKPTCSDT thi thoat khoi for
                                                                if (kkpt1.Id == kkpt2.Id)
                                                                {
                                                                    co = true;
                                                                    break;
                                                                }
                                                            }
                                                            if (co)
                                                            {
                                                                //thoat vong for thu 1
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                else //neu khong co KKPT trong CTHD
                                                {

                                                    if (dsKKPTCSDT == null || dsKKPTCSDT.Entities.Count() == 0)
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                if (co == false)
                                                    continue;

                                                // Nhom cu ly
                                                co = false;

                                                EntityCollection dsNHomCL = RetrieveNNRecord(service, "new_nhomculy",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomculy",
                                                    new ColumnSet(new string[] { "new_nhomculyid" }),
                                                    "new_chinhsachdautuid", a.Id);
                                                if (thuadatObj.Attributes.Contains("new_nhomculy"))
                                                {
                                                    Guid nhomclId =
                                                        thuadatObj.GetAttributeValue<EntityReference>("new_nhomculy").Id;
                                                    Entity nhomCL = service.Retrieve("new_nhomculy", nhomclId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsNHomCL != null && dsNHomCL.Entities.Count > 0)
                                                    {
                                                        foreach (Entity nhomCL1 in dsNHomCL.Entities)
                                                        {
                                                            if (nhomCL.Id == nhomCL1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                else //neu khong co NHomCL trong CTHD
                                                {

                                                    if (dsNHomCL == null || dsNHomCL.Entities.Count() == 0)
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                if (co == false)
                                                    continue;

                                                // Mo hinh khuyen nong
                                                co = false;

                                                EntityCollection dsMHKN = RetrieveNNRecord(service,
                                                    "new_mohinhkhuyennong", "new_chinhsachdautu",
                                                    "new_new_chinhsachdautu_new_mohinhkhuyennong",
                                                    new ColumnSet(new string[] { "new_mohinhkhuyennongid" }),
                                                    "new_chinhsachdautuid", a.Id);

                                                if (ChiTietHD.Attributes.Contains("new_thamgiamohinhkhuyennong"))
                                                {
                                                    EntityReference mhknEntityRef =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_thamgiamohinhkhuyennong");
                                                    Guid mhknId = mhknEntityRef.Id;
                                                    Entity mhkn = service.Retrieve("new_mohinhkhuyennong", mhknId,
                                                        new ColumnSet(new string[] { "new_name" }));

                                                    if (dsMHKN != null && dsMHKN.Entities.Count() > 0)
                                                    {
                                                        foreach (Entity mhkn1 in dsMHKN.Entities)
                                                        {
                                                            if (mhkn.Id == mhkn1.Id)
                                                            {
                                                                co = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                else //neu khong co MNKH trong CTHD
                                                {
                                                    if (dsMHKN == null || dsMHKN.Entities.Count() == 0)
                                                    {
                                                        co = true;
                                                    }
                                                }
                                                if (co == false)
                                                    continue;

                                                // NHom nang suat
                                                co = false;

                                                EntityCollection dsNhomNS = RetrieveNNRecord(service, "new_nhomnangsuat",
                                                    "new_chinhsachdautu", "new_new_chinhsachdautu_new_nhomnangsuat",
                                                    new ColumnSet(new string[]
                                                        {"new_nhomnangsuatid", "new_nangsuattu", "new_nangsuatden"}),
                                                    "new_chinhsachdautuid", a.Id);

                                                if (ChiTietHD.Attributes.Contains("new_khachhang"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>("new_khachhang").Id;
                                                    Entity khObj = service.Retrieve("contact", khId,
                                                        new ColumnSet(new string[] { "fullname", "new_nangsuatbinhquan" }));

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                if (mhkn1.Attributes.Contains("new_nangsuattu") &&
                                                                    mhkn1.Attributes.Contains("new_nangsuatden"))
                                                                {
                                                                    decimal nangsuattu =
                                                                        mhkn1.GetAttributeValue<decimal>(
                                                                            "new_nangsuattu");
                                                                    decimal nangsuatden =
                                                                        mhkn1.GetAttributeValue<decimal>(
                                                                            "new_nangsuatden");

                                                                    if ((nangsuatbq >= nangsuattu) &&
                                                                        (nangsuatbq <= nangsuatden))
                                                                    {
                                                                        co = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                }
                                                if (ChiTietHD.Attributes.Contains("new_khachhangdoanhnghiep"))
                                                {
                                                    Guid khId =
                                                        ChiTietHD.GetAttributeValue<EntityReference>(
                                                            "new_khachhangdoanhnghiep").Id;
                                                    Entity khObj = service.Retrieve("account", khId,
                                                        new ColumnSet(new string[] { "name", "new_nangsuatbinhquan" }));

                                                    if (khObj.Attributes.Contains("new_nangsuatbinhquan"))
                                                    {
                                                        decimal nangsuatbq =
                                                            khObj.GetAttributeValue<decimal>("new_nangsuatbinhquan");
                                                        if (dsNhomNS != null && dsNhomNS.Entities.Count() > 0)
                                                        {
                                                            foreach (Entity mhkn1 in dsNhomNS.Entities)
                                                            {
                                                                if (mhkn1.Attributes.Contains("new_nangsuattu") &&
                                                                    mhkn1.Attributes.Contains("new_nangsuatden"))
                                                                {
                                                                    decimal nangsuattu =
                                                                        mhkn1.GetAttributeValue<decimal>(
                                                                            "new_nangsuattu");
                                                                    decimal nangsuatden =
                                                                        mhkn1.GetAttributeValue<decimal>(
                                                                            "new_nangsuatden");

                                                                    if ((nangsuatbq >= nangsuattu) &&
                                                                        (nangsuatbq <= nangsuatden))
                                                                    {
                                                                        co = true;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                    else
                                                    {
                                                        if (dsNhomNS == null || dsNhomNS.Entities.Count() == 0)
                                                        {
                                                            co = true;
                                                        }
                                                    }
                                                }

                                                if (co == false)
                                                    continue;

                                                mCSDTung = a;
                                                break;
                                            }
                                            if (mCSDTung != null && mCSDTung.Id != Guid.Empty)
                                                listCSDT.Add(mCSDTung.ToEntityReference());
                                        }

                                        traceService.Trace("vi trí add  ứng");
                                        //----END---- Tìm CSDT ứng --------------------

                                        service.Associate("new_thuadatcanhtac", ChiTietHD.Id,
                                            new Relationship("new_new_chitiethddtmia_new_chinhsachdautu"), listCSDT);

                                        traceService.Trace("add  xong het");
                                    } // foreach (Entity plgocto in dsPLGocsangTo.Entities)
                                }
                            } // End if Loại phụ lục chuyển gốc sang tơ
                        } // if (PhulucHD.Contains("new_tinhtrangduyet") && PhulucHD.GetAttributeValue<OptionSetValue>("new_tinhtrangduyet").Value.ToString() == "100000005" && PhulucHD.Contains("new_loaiphuluc"))
                    } // if (context.MessageName.ToUpper() == "UPDATE")
                }
            }
        }

        public static EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection collRecords = crmservices.RetrieveMultiple(query);

            return collRecords;
        }

        public static EntityCollection FindTLTHVbosung(IOrganizationService crmservices, Entity CSDTbosung)
        {
            QueryExpression q = new QueryExpression("new_tilethuhoivon");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_chinhsachdautubosung", ConditionOperator.Equal, CSDTbosung.Id));
            q.Orders.Add(new OrderExpression("new_nam", OrderType.Ascending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTthamcanh(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000001));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTtuoi(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000002));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTbocla(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000003));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindCSDTung(IOrganizationService crmservices, Entity ctHDDT)
        {
            Entity HD = crmservices.Retrieve("new_hopdongdautumia", ((EntityReference)ctHDDT["new_hopdongdautumia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
            Entity VDT = crmservices.Retrieve("new_vudautu", ((EntityReference)HD["new_vudautu"]).Id, new ColumnSet(new string[] { "new_name" }));

            QueryExpression q = new QueryExpression("new_chinhsachdautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ngayapdung", ConditionOperator.LessEqual, ctHDDT["createdon"]));
            q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, VDT.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_loaihopdong", ConditionOperator.Equal, 100000000));
            q.Criteria.AddCondition(new ConditionExpression("new_mucdichdautu", ConditionOperator.Equal, 100000004));
            q.Orders.Add(new OrderExpression("new_ngayapdung", OrderType.Descending));

            EntityCollection entc = crmservices.RetrieveMultiple(q);
            return entc;
        }

        public static EntityCollection FindchitietHD(IOrganizationService crmservices, Entity HD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_thuadatcanhtac'>
                        <attribute name='new_name' />
                        <attribute name='new_dautuhoanlai' />
                        <attribute name='statuscode' />
                        <attribute name='new_giongmia' />
                        <attribute name='new_loaigocmia' />
                        <attribute name='new_thuadatcanhtacid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, HD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindPLHDGocsangTo(IOrganizationService crmservices, Entity PLHD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_phuluchopdong_gocsangto'>
                        <attribute name='new_name' />
                        <attribute name='new_dongiahopdongkhl' />
                        <attribute name='new_dongiahopdong' />
                        <attribute name='new_dongiadautukhonghoanlai' />
                        <attribute name='new_dongiadautuhoanlai' />
                        <attribute name='new_dinhmucdautukhonghoanlai' />
                        <attribute name='new_dinhmucdautuhoanlai' />
                        <attribute name='new_chitiethopdongdautumia' />
                        <attribute name='new_thoihanthuedatconlai' />
<attribute name='new_loaitrong' />
<attribute name='new_mucdichsanxuatmia' />
<attribute name='new_giongmiadangky' />
                        <attribute name='new_phuluchopdong_gocsangtoid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_phuluchopdong' operator='eq' uitype='new_phuluchopdong' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, PLHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindtyleTHV(IOrganizationService crmservices, Entity CSDT)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                              <entity name='new_tilethuhoivon'>
                                                <attribute name='new_name' />
                                                <attribute name='new_phantramtilethuhoi' />
                                                <attribute name='new_nam' />
                                                <attribute name='new_chinhsachdautu' />
                                                <attribute name='new_chinhsachdautubosung' />
                                                <attribute name='new_tilethuhoivonid' />
                                                <order attribute='new_nam' descending='false' />
                                                <link-entity name='new_chinhsachdautu' from='new_chinhsachdautuid' to='new_chinhsachdautu' alias='ac'>
                                                  <filter type='and'>
                                                    <condition attribute='statecode' operator='eq' value='0' />
                                                    <condition attribute='new_chinhsachdautuid' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                                  </filter>
                                                </link-entity>
                                              </entity>
                                            </fetch>";
            fetchXml = string.Format(fetchXml, CSDT.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindthongsoVDT(IOrganizationService crmservices, Entity Vudt)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                  <entity name='new_thongsotheovudautu'>
                                    <attribute name='new_name' />
                                    <attribute name='createdon' />
                                    <attribute name='new_vudautu' />
                                    <attribute name='new_loai' />
                                    <attribute name='new_giatri' />
                                    <attribute name='new_giatien' />
                                    <attribute name='new_apdungtu' />
                                    <attribute name='new_thongsotheovudautuid' />
                                    <order attribute='new_name' descending='false' />
                                    <filter type='and'>
                                      <condition attribute='statecode' operator='eq' value='0' />
                                      <condition attribute='new_vudautu' operator='eq' uitype='new_vudautu' value='{0}' />
                                    </filter>
                                  </entity>
                                </fetch>";
            fetchXml = string.Format(fetchXml, Vudt.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindCSDTBS(IOrganizationService crmservices, Entity Vudt, DateTime ngaytao)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                          <entity name='new_chinhsachdautuchitiet'>
                                            <attribute name='new_chinhsachdautuchitietid' />
                                            <attribute name='new_name' />
                                            <attribute name='createdon' />
                                            <attribute name='new_nhomkhachhang' />
                                            <attribute name='new_giongmia' />
                                            <attribute name='new_nhomnangsuat' />
                                            <attribute name='new_khuyenkhichphattrien' />
                                            <attribute name='new_mohinhkhuyennong' />
                                            <attribute name='new_nhomculy' />
                                            <attribute name='new_vungdialy' />
                                            <attribute name='new_sotienbosung' />
                                            <attribute name='new_sotienbosung_khl' />
                                            <attribute name='new_bosungphanbon' />
                                            <attribute name='new_bosungtienmat' />
                                            <order attribute='new_name' descending='false' />
                                            <filter type='and'>
                                              <condition attribute='statecode' operator='eq' value='0' />
                                              <condition attribute='new_vudautu' operator='eq' value='{0}' />
                                              <condition attribute='new_tungay' operator='on-or-before' value='{1}' />
                                              <condition attribute='new_denngay' operator='on-or-after' value='{2}' />
                                            </filter>
                                          </entity>
                                        </fetch>";
            fetchXml = string.Format(fetchXml, Vudt.Id, ngaytao, ngaytao);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        public static EntityCollection FindChitietHDtheoPL(IOrganizationService crmservices, Entity PLHD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_thuadatcanhtac'>
                        <attribute name='new_name' />
                        <attribute name='new_hopdongdautumia' />
                        <attribute name='statuscode' />
                        <attribute name='new_thuadatcanhtacid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_phuluchopdongid' operator='eq' uitype='new_phuluchopdong' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";
            fetchXml = string.Format(fetchXml, PLHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));
            return entc;
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression(LogicalOperator.And);
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.NotEqual, 100000007));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

        int GetCurrentPos(Entity hddtm)
        {
            QueryExpression q = new QueryExpression("new_thuadatcanhtac");
            q.ColumnSet = new ColumnSet(new string[] { "new_current" });
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hddtm.Id));
            q.AddOrder("new_current", OrderType.Descending);

            EntityCollection entc = service.RetrieveMultiple(q);
            int n = entc.Entities.Count;

            if (n > 0)
            {
                Entity first = entc.Entities[n - 1];

                int currentpos = first.Contains("new_current") ? (int)first["new_current"] : 0;

                return ++currentpos;
            }
            else return 1;
        }
    }
}
