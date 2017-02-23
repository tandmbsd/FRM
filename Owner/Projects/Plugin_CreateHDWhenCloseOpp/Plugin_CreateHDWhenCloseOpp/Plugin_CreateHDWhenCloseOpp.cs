using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Plugin_CreateHDWhenCloseOpp
{
    // moi nhat 
    public class Plugin_CreateHDWhenCloseOpp : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.Depth > 1)
                return;

            Entity target = (Entity)context.InputParameters["Target"];
            string vl = "";

            if (target.Contains("new_tinhtrangduyet") && ((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000004) // dang tham dinh
            {
                Entity Opp = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                string[] loaihd = Opp["new_loaihopdong_vl"].ToString().Split(',');
                vl = Opp["new_loaihopdong_vl"].ToString();

                string loai = "";
                string loaihdthuedat;

                foreach (string a in loaihd)
                {
                    Entity Hd = new Entity();
                    //Hd["new_name"] = Opp.Contains("name") ? Opp["name"] : null;
                    Hd["new_vudautu"] = Opp.Contains("new_vudautu") ? Opp["new_vudautu"] : null;
                    Hd["new_cohoi"] = new EntityReference(Opp.LogicalName, Opp.Id);
                    Hd["new_tram"] = Opp.Contains("new_tram") ? Opp["new_tram"] : null;
                    Hd["new_canbonongvu"] = Opp.Contains("new_canbonongvu") ? Opp["new_canbonongvu"] : null;
                    Hd["transactioncurrencyid"] = Opp["transactioncurrencyid"];
                    loai = "";
                    Guid Hdid;
                    if (a == "100000000")
                        loai = "100000002";
                    else if (a == "100000002")
                        loai = "100000000";

                    if (loai == "100000002")
                    {
                        //Hợp đồng đầu tư mía
                        Entity temp = null;

                        if (Opp.Contains("new_quocgia"))
                            Hd["new_quocgia"] = Opp["new_quocgia"];

                        if (Opp.Contains("new_loaihopdongdautumia"))
                            Hd["new_loaihopdong"] = Opp["new_loaihopdongdautumia"];

                        traceService.Trace("A");
                        if (Opp.Contains("new_loainguoithuake") && ((OptionSetValue)Opp["new_loainguoithuake"]).Value == 100000000) // ntk cũ                        
                            Hd["new_nguoithuake"] = Opp["new_nguoithuake"];
                        else if (Opp.Contains("new_loainguoithuake") && ((OptionSetValue)Opp["new_loainguoithuake"]).Value == 100000001) // ntk cũ                        
                        {
                            Entity contact = new Entity("contact");
                            StringBuilder name = new StringBuilder();

                            string fullname = (string)Opp["new_hddtmia_nguoithuake"];

                            string[] words = fullname.Split(' ');

                            for (int i = 1; i < words.Length; i++)
                            {
                                string t = words[i];
                                name.Append(t + " ");
                            }
                            traceService.Trace("A");
                            contact["firstname"] = words[0];
                            contact["lastname"] = name.ToString().Trim();
                            traceService.Trace("B");
                            contact["new_loaikhachhang"] = new OptionSetValue(100000004);
                            contact["new_loaikh"] = "100000004";
                            contact["new_chiconamsinh"] = Opp["new_chiconamsinh"];
                            contact["new_nhomkhachhang"] = GetNKHBac().ToEntityReference();

                            if ((bool)Opp["new_chiconamsinh"] == true)
                                contact["new_namsinh"] = Opp.Contains("new_namsinh_htdtmia") ? Opp["new_namsinh_htdtmia"] : null;
                            else
                                contact["birthdate"] = Opp.Contains("new_ngaysinh_hddtmia") ? Opp["new_ngaysinh_hddtmia"] : null;
                            traceService.Trace("C");
                            contact["new_socmnd"] = Opp.Contains("new_hddtmia_cmndnguoithuake") ? Opp["new_hddtmia_cmndnguoithuake"] : null;
                            contact["new_noicap"] = Opp.Contains("new_noicap_hddtmia") ? Opp["new_noicap_hddtmia"] : null;
                            contact["new_ngaycap"] = Opp.Contains("new_ngaycap_hddtmia") ? Opp["new_ngaycap_hddtmia"] : null;
                            contact["new_diachithuongtru"] = Opp.Contains("new_diachithuongtru_hddtmia") ? Opp["new_diachithuongtru_hddtmia"] : null;
                            contact["mobilephone"] = Opp.Contains("new_dienthoai_hddtmia") ? Opp["new_dienthoai_hddtmia"] : null;
                            traceService.Trace("D");
                            Guid contactID = service.Create(contact);

                            Hd["new_nguoithuake"] = new EntityReference("contact", contactID);
                        }
                        traceService.Trace("tao xong người thừa kế");

                        if (Opp.Contains("new_moiquanhehhdtmia"))
                            Hd["new_moiquanhechuhopdongvanguoithuake"] = Opp["new_moiquanhehhdtmia"];

                        StringBuilder str = new StringBuilder();
                        str.Append("HDDT");

                        traceService.Trace("hop dong dau tu mia");
                        Hd.LogicalName = "new_hopdongdautumia";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_khachhang"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                                str.Append("-" + temp["fullname"].ToString());

                            if (temp.Contains("new_socmnd"))
                                str.Append("-" + temp["new_socmnd"].ToString());
                        }
                        else
                        {
                            Hd["new_khachhangdoanhnghiep"] = Opp["customerid"];

                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                                str.Append("-" + temp["name"].ToString());

                            if (temp.Contains("new_sogpkd"))
                                str.Append("-" + temp["new_sogpkd"].ToString());
                        }
                        Hdid = service.Create(Hd);
                        traceService.Trace("Tạo xong HĐ");
                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet("new_name", "new_masohopdong"));

                        if (newHD.Contains("new_masohopdong"))
                            str.Append("-" + newHD["new_masohopdong"].ToString());

                        newHD["new_name"] = str.ToString();
                        service.Update(newHD);

                        List<Entity> lstCTCH_DTHD = RetrieveMultiRecord(service, "new_chitietcohoi_dientichhopdong",
                            new ColumnSet(true), "new_cohoi", Opp.Id);

                        traceService.Trace("số chi tiết cơ hội: " + lstCTCH_DTHD.Count.ToString());

                        foreach (Entity en in lstCTCH_DTHD)
                        {
                            int loaithuadat = ((OptionSetValue)en["new_loaithuadat"]).Value;
                            Guid idThuadat = Guid.Empty;
                            Entity thuadatNew = new Entity("new_thuadat");
                            traceService.Trace(loaithuadat.ToString());
                            if (loaithuadat == 100000000) // thua dat moi
                            {
                                thuadatNew["new_dientich"] = en.Contains("new_dientichmia") ? en["new_dientichmia"] : 0;
                                traceService.Trace("dien tich");
                                if (en.Contains("new_nhomdat"))
                                    thuadatNew["new_nhomdat"] = en["new_nhomdat"];

                                if (en.Contains("new_nguongocdat"))
                                    thuadatNew["new_loaisohuudat"] = en["new_nguongocdat"];
                                traceService.Trace("nguoc goc dat");

                                if (en.Contains("new_vungdialy"))
                                    thuadatNew["new_vungdialy"] = en["new_vungdialy"];

                                if (en.Contains("new_diachi"))
                                    thuadatNew["new_diachi"] = en["new_diachi"];

                                traceService.Trace("diachi");
                                thuadatNew["new_culyvanchuyen"] = en.Contains("new_culyvenhamay") ? en["new_culyvenhamay"] : new decimal(0);
                                thuadatNew["new_culy"] = en.Contains("new_culytrungchuyen") ? en["new_culytrungchuyen"] : new decimal(0);
                                thuadatNew["new_tram"] = Opp.Contains("new_tram") ? Opp["new_tram"] : null;
                                thuadatNew["new_canbonongvu"] = Opp.Contains("new_canbonongvu") ? Opp["new_canbonongvu"] : null;

                                if (en.Contains("new_chusohuukh"))
                                    thuadatNew["new_chusohuuchinhtd"] = en["new_chusohuukh"];
                                else if (en.Contains("new_chusohuukhdn"))
                                    thuadatNew["new_chusohuuchinhtdkhdn"] = en["new_chusohuukhdn"];

                                idThuadat = service.Create(thuadatNew);

                                Entity t = service.Retrieve("new_thuadat", idThuadat, new ColumnSet(new string[] { "new_name" }));
                                //set thua dat for chi tiet co hoi dien tich hop dong
                                Entity updateCTCHDTHD = service.Retrieve(en.LogicalName, en.Id,
                                    new ColumnSet(new string[] { "new_thuadatmoi" }));

                                updateCTCHDTHD["new_thuadatmoi"] = t["new_name"].ToString();
                                service.Update(updateCTCHDTHD);
                            }
                            else if (loaithuadat == 100000001) // thua dat cu                            
                                idThuadat = ((EntityReference)en["new_thuadatcu"]).Id;

                            traceService.Trace("Tạo xong thửa đất");
                            Entity thuadatcanhtac = new Entity("new_thuadatcanhtac");
                            str.Clear();
                            Entity td = service.Retrieve("new_thuadat", idThuadat, new ColumnSet(new string[] { "new_name" }));

                            str.Append("CTHDDT");
                            if (newHD.Contains("new_masohopdong"))
                                str.Append("-" + newHD["new_masohopdong"].ToString());
                            if (td.Contains("new_name"))
                                str.Append("-" + td["new_name"].ToString());

                            thuadatcanhtac["new_name"] = str.ToString();
                            thuadatcanhtac["new_dientichhopdong"] = en.Contains("new_dientichmia") ? en["new_dientichmia"] : 0;
                            thuadatcanhtac["new_dientichthucte"] = new decimal(0);

                            if (en.Contains("new_vutrong"))
                                thuadatcanhtac["new_vutrong"] = en["new_vutrong"];
                            traceService.Trace("a");

                            if (en.Contains("new_loaigocmia"))
                            {
                                thuadatcanhtac["new_loaigocmia"] = en["new_loaigocmia"];

                                if (((OptionSetValue)en["new_loaigocmia"]).Value == 100000001)
                                    thuadatcanhtac["new_luugoc"] = en["new_luug0c"];
                            }

                            traceService.Trace("b");
                            if (en.Contains("new_nguongocdat"))
                                thuadatcanhtac["new_loaisohuudat"] = en["new_nguongocdat"];

                            traceService.Trace("c");
                            thuadatcanhtac["new_hopdongdautumia"] = new EntityReference(Hd.LogicalName, Hdid);

                            if (en.Contains("new_loaitrong"))
                                thuadatcanhtac["new_loaitrong"] = en["new_loaitrong"];

                            if (en.Contains("new_giongmia"))
                                thuadatcanhtac["new_giongmia"] = en["new_giongmia"];

                            if (en.Contains("new_ngaytrongdukien"))
                                thuadatcanhtac["new_ngaytrongdukien"] = en["new_ngaytrongdukien"];

                            if (temp.LogicalName == "contact")
                                thuadatcanhtac["new_khachhang"] = temp.ToEntityReference();

                            else if (temp.LogicalName == "account")
                                thuadatcanhtac["new_khachhangdoanhnghiep"] = temp.ToEntityReference();

                            if (en.Contains("new_thanhtien"))
                                thuadatcanhtac["new_dautuhoanlai"] = en["new_thanhtien"];

                            if (en.Contains("new_dinhmuc"))
                                thuadatcanhtac["new_dongiahopdong"] = en["new_dinhmuc"];

                            if (en.Contains("new_mucdichsanxuatmia"))
                                thuadatcanhtac["new_mucdichsanxuatmia"] = en["new_mucdichsanxuatmia"];

                            if (en.Contains("new_tuoimia"))
                                thuadatcanhtac["new_tuoimia"] = en["new_tuoimia"];

                            thuadatcanhtac["new_thuadat"] = new EntityReference("new_thuadat", idThuadat);

                            service.Create(thuadatcanhtac);
                        }
                    }
                    else if (loai == "100000000")
                    {
                        //Hợp đồng đầu tư thuê đất
                        traceService.Trace("HD thue dat");
                        Entity temp = null;
                        StringBuilder str = new StringBuilder();
                        str.Append("HDTD");

                        Hd.LogicalName = "new_hopdongthuedat";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_khachhang"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                                str.Append("-" + temp["fullname"].ToString());

                            if (temp.Contains("new_socmnd"))
                                str.Append("-" + temp["new_socmnd"].ToString());

                        }
                        else
                        {
                            Hd["new_khachhangdoanhnghiep"] = Opp["customerid"];
                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                            {
                                str.Append("-" + temp["name"].ToString());
                            }

                            if (temp.Contains("new_sogpkd"))
                            {
                                str.Append("-" + temp["new_sogpkd"].ToString());
                            }
                        }

                        if (Opp.Contains("new_quocgia"))
                        {
                            Hd["new_quocgia"] = new EntityReference("new_quocgia", ((EntityReference)Opp["new_quocgia"]).Id);

                            Entity quocgia = service.Retrieve("new_quocgia", ((EntityReference)Opp["new_quocgia"]).Id,
                                new ColumnSet(new string[] { "new_shortname" }));
                            string srtname = (string)quocgia["new_shortname"];

                            if (srtname == "VN")
                                Hd["new_loaithue"] = new OptionSetValue(100000000); // trong nuoc
                            else
                                Hd["new_loaithue"] = new OptionSetValue(100000001); // ngoai nuoc
                        }

                        Hdid = service.Create(Hd);
                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet("new_name", "new_mahopdong"));

                        if (newHD.Contains("new_mahopdong"))
                            str.Append("-" + newHD["new_mahopdong"].ToString());

                        newHD["new_name"] = str.ToString();
                        service.Update(newHD);
                        traceService.Trace("Update thành công hợp đồng");
                        List<Entity> lstCTCH_DTDT = RetrieveMultiRecord(service, "new_chitietcohoi_dientichdatthue",
                            new ColumnSet(true), "new_cohoi", Opp.Id);

                        Dictionary<string, List<Entity>> groupChitietthuedat = new Dictionary<string, List<Entity>>();
                        Entity KH = null;
                        string cmnd = "";

                        foreach (Entity en in lstCTCH_DTDT)
                        {
                            if (en.Contains("new_loaisohuucd") && ((OptionSetValue)en["new_loaisohuucd"]).Value == 100000000) // chu dat moi
                            {
                                traceService.Trace("loai so huu cd moi");
                                cmnd = en.Contains("new_socmndcd") ? (string)en["new_socmndcd"] : "";

                                if (!groupChitietthuedat.ContainsKey(cmnd))
                                    groupChitietthuedat.Add(cmnd, new List<Entity>());

                                groupChitietthuedat[cmnd].Add(en);
                            }
                            else if (en.Contains("new_loaisohuucd") && ((OptionSetValue)en["new_loaisohuucd"]).Value == 100000001) // chu dat cu
                            {
                                traceService.Trace("loai so huu cd cu");
                                if (en.Contains("new_chudatkh"))
                                {
                                    KH = service.Retrieve("contact", ((EntityReference)en["new_chudatkh"]).Id,
                                        new ColumnSet(new string[] { "new_socmnd" }));
                                }
                                else if (en.Contains("new_chudatkhdn"))
                                {
                                    KH = service.Retrieve("contact", ((EntityReference)en["new_chudatkh"]).Id,
                                        new ColumnSet(new string[] { "new_socmnd" }));
                                }

                                cmnd = KH.Contains("new_socmnd") ? (string)KH["new_socmnd"] : "";

                                if (!groupChitietthuedat.ContainsKey(cmnd))
                                    groupChitietthuedat.Add(cmnd, new List<Entity>());

                                groupChitietthuedat[cmnd].Add(en);
                            }
                        }
                        traceService.Trace("aaaa");
                        foreach (string key in groupChitietthuedat.Keys)
                        {
                            int i = 0;
                            Guid idDatthue = Guid.Empty;
                            foreach (Entity en in groupChitietthuedat[key])
                            {
                                Guid idThuadat = Guid.Empty;

                                int sonamthue = 0;
                                decimal dientich = 0;
                                decimal dongiathuethucte = 0;

                                if (i == 0)
                                {
                                    #region create chi tiet hd thue dat                                    
                                    Entity datthue = new Entity("new_datthue");

                                    str.Clear();
                                    str.Append("CTHDTD");
                                    if (newHD.Contains("new_mahopdong"))
                                        str.Append("-" + newHD["new_mahopdong"].ToString());

                                    if (temp.Contains("new_socmnd"))
                                        str.Append("-" + temp["new_socmnd"].ToString());

                                    if (temp.Contains("fullname"))
                                        str.Append("-" + temp["fullname"].ToString());

                                    if (en.Contains("new_vubatdauthue"))
                                        datthue["new_vubatdauthue"] = en["new_vubatdauthue"];

                                    datthue["new_name"] = str.ToString();

                                    if (((OptionSetValue)en["new_loaisohuucd"]).Value == 100000000) // chu dat moi
                                    {
                                        Entity contact = new Entity("contact");
                                        StringBuilder name = new StringBuilder();

                                        if (!en.Contains("new_hotencd"))
                                            throw new Exception("Chủ đất mới không có tên ");

                                        if (!en.Contains("new_socmnd"))
                                            throw new Exception("Chủ đất mới không có số CMND ");

                                        string fullname = (string)en["new_hotencd"];
                                        string[] words = fullname.Split(' ');

                                        for (int k = 1; k < words.Length; k++)
                                        {
                                            string t = words[k];
                                            name.Append(t + " ");
                                        }
                                        traceService.Trace("A");

                                        contact["firstname"] = words[0];
                                        contact["lastname"] = name.ToString().Trim();
                                        contact["new_socmnd"] = (string)en["new_socmndcd"]; ;
                                        traceService.Trace("A1");
                                        contact["new_loaikhachhang"] = new OptionSetValue(100000006);
                                        contact["new_loaikh"] = "100000006";
                                        traceService.Trace("A2");
                                        contact["new_nhomkhachhang"] = GetNKHBac().ToEntityReference();
                                        contact["new_chiconamsinh"] = false;
                                        traceService.Trace("A3");
                                        //contact["birthdate"] = (DateTime)en["new_ngaysinhcd"];
                                        //contact["new_noicap"] = en.Contains("new_noicapcd") ? (string)en["new_noicapcd"] : "";
                                        //contact["new_ngaycap"] = (DateTime)en["new_ngaycapcd"];
                                        //contact["new_diachithuongtru"] = en.Contains("new_diachithuongtrucd") ? (string)en["new_diachithuongtrucd"] : "";
                                        contact["mobilephone"] = en.Contains("new_dienthoaicd") ? (string)en["new_dienthoaicd"] : "";

                                        Guid id_Contact = service.Create(contact);
                                        datthue["new_benchothuedatkh"] = new EntityReference("contact", id_Contact);
                                    }
                                    else if (((OptionSetValue)en["new_loaisohuucd"]).Value == 100000001) // chu dat cu
                                    {
                                        if (en.Contains("new_chudatkh"))
                                            datthue["new_benchothuedatkh"] = en["new_chudatkh"];
                                        else if (en.Contains("new_chudatkhdn"))
                                            datthue["new_benchothuedatkhdn"] = en["new_chudatkhdn"];
                                    }

                                    if (((OptionSetValue)en["new_loainguoithuake"]).Value == 100000000) // kh mới
                                    {
                                        Entity contact = new Entity("contact");
                                        StringBuilder name = new StringBuilder();

                                        if (!en.Contains("new_hoten"))
                                            throw new Exception("Người thừa kế mới không có tên ");

                                        if (!en.Contains("new_socmnd"))
                                            throw new Exception("Người thừa kế mới không có số CMND ");

                                        string fullname = (string)en["new_hoten"];
                                        string[] words = fullname.Split(' ');

                                        for (int k = 1; k < words.Length; k++)
                                        {
                                            string t = words[k];
                                            name.Append(t + " ");
                                        }
                                        traceService.Trace("B");

                                        contact["firstname"] = words[0];
                                        contact["lastname"] = name.ToString().Trim();
                                        contact["new_socmnd"] = (string)en["new_socmnd"]; ;
                                        traceService.Trace("B1");
                                        contact["new_loaikhachhang"] = new OptionSetValue(100000006);
                                        contact["new_loaikh"] = "100000006";
                                        traceService.Trace("B2");
                                        contact["new_nhomkhachhang"] = GetNKHBac().ToEntityReference();
                                        contact["new_chiconamsinh"] = false;
                                        traceService.Trace("B3");
                                        //contact["birthdate"] = (DateTime)en["new_ngaysinh"];
                                        //contact["new_noicap"] = en.Contains("new_noicap") ? (string)en["new_noicap"] : "";
                                        //contact["new_ngaycap"] = (DateTime)en["new_ngaycap"];
                                        //contact["new_diachithuongtru"] = en.Contains("new_diachithuongtrucd") ? (string)en["new_diachithuongtrucd"] : "";
                                        //contact["mobilephone"] = en.Contains("new_dienthoai") ? (string)en["new_dienthoai"] : "";

                                        Guid id_Contact = service.Create(contact);
                                        datthue["new_benchothuedatthuakekh"] = new EntityReference("contact", id_Contact);
                                        traceService.Trace("tao thanh cong ben cho thue dat");
                                    }
                                    else if (((OptionSetValue)en["new_loainguoithuake"]).Value == 100000001) // kh cũ
                                    {
                                        if (en.Contains("new_nguoithuake"))
                                            datthue["new_benchothuedatthuakekh"] = en["new_nguoithuake"];
                                    }

                                    if (en.Contains("new_mqhchudatnguoithue"))
                                        datthue["new_moiquanhechudatnguoithue"] = en["new_mqhchudatnguoithue"];

                                    if (en.Contains("new_mqhchudatthuake"))
                                        datthue["new_moiquanhegiuachudatthuake"] = en["new_mqhchudatthuake"];

                                    datthue["new_hopdongthuedat"] = new EntityReference(Hd.LogicalName, Hdid);

                                    idDatthue = service.Create(datthue);
                                    traceService.Trace("Tạo thành công chi tiết hd thuê đất");
                                    traceService.Trace(idDatthue.ToString());
                                    #endregion
                                }

                                traceService.Trace(idDatthue.ToString());
                                int loaithuadat = ((OptionSetValue)en["new_loaithuadat"]).Value;

                                if (loaithuadat == 100000000) // thua dat cu
                                {
                                    traceService.Trace("td cũ");
                                    idThuadat = ((EntityReference)en["new_thuadatcu"]).Id;
                                }
                                else if (loaithuadat == 100000001) // thua dat moi
                                {
                                    traceService.Trace("td mới");

                                    string masothua = en.Contains("new_sothua") ? (string)en["new_sothua"] : "";
                                    Entity thuadat = GetThuadatFromMasoThua(Opp, masothua);

                                    if (thuadat == null) throw new Exception("Không tồn tại mã số thửa");

                                    idThuadat = thuadat.Id;
                                    traceService.Trace(idThuadat.ToString());

                                    Entity t = service.Retrieve("new_thuadat", idThuadat, new ColumnSet(new string[] { "new_name" }));
                                    //set thua dat for chi tiet co hoi dien tich hop dong
                                    Entity updateCTCHDTHD = service.Retrieve(en.LogicalName, en.Id,
                                        new ColumnSet(new string[] { "new_thuadatmoi" }));

                                    updateCTCHDTHD["new_thuadatmoi"] = t["new_name"].ToString();
                                    service.Update(updateCTCHDTHD);
                                }
                                traceService.Trace("tạo thành công thửa đất");

                                Entity cthdthuedat_thuadat = new Entity("new_chitiethdthuedat_thuadat");
                                str.Clear();
                                str.Append("CTHDTD");

                                if (newHD.Contains("new_mahopdong"))
                                    str.Append("-" + newHD["new_mahopdong"].ToString());

                                Entity td = service.Retrieve("new_thuadat", idThuadat,
                                    new ColumnSet(new string[] { "new_name" }));

                                if (td.Contains("new_name"))
                                    str.Append("-" + td["new_name"].ToString());

                                if (en.Contains("new_sonamthue"))
                                {
                                    cthdthuedat_thuadat["new_sonamthuedat"] = en["new_sonamthue"];
                                    sonamthue = (int)en["new_sonamthue"];
                                }
                                traceService.Trace("name");
                                cthdthuedat_thuadat["new_name"] = str.ToString();
                                cthdthuedat_thuadat["new_thuadat"] = new EntityReference("new_thuadat", idThuadat);
                                cthdthuedat_thuadat["new_chitiethdthuedat"] = new EntityReference("new_datthue", idDatthue);

                                traceService.Trace("dien tich dat");
                                if (en.Contains("new_dientichdat"))
                                {
                                    cthdthuedat_thuadat["new_dientichthuehd"] = en["new_dientichdat"];
                                    dientich = (decimal)en["new_dientichdat"];
                                }

                                traceService.Trace("dinh muc dau tu");

                                if (en.Contains("new_dinhmucdautu"))
                                {
                                    cthdthuedat_thuadat["new_dongiahopdong"] = en["new_dinhmucdautu"];
                                }

                                traceService.Trace("don gia thue thuc te");
                                if (en.Contains("new_dongiathuethucte"))
                                {
                                    cthdthuedat_thuadat["new_dongiathuethucte"] = en["new_dongiathuethucte"];
                                    dongiathuethucte = ((Money)en["new_dongiathuethucte"]).Value;
                                }

                                traceService.Trace("vu bat dau thue");
                                if (en.Contains("new_vubatdauthue"))
                                    cthdthuedat_thuadat["new_vubatdauthue"] = en["new_vubatdauthue"];

                                traceService.Trace("thanh tien");
                                if (en.Contains("new_thanhtien"))
                                    cthdthuedat_thuadat["new_sotiendautu"] = en["new_thanhtien"];

                                if (en.Contains("new_caytrongtruockhithue"))
                                    cthdthuedat_thuadat["new_hientrangcaytrong"] = en["new_caytrongtruockhithue"];

                                cthdthuedat_thuadat["new_sotienthuethucthue"] = new Money(sonamthue * dientich * dongiathuethucte);
                                service.Create(cthdthuedat_thuadat);
                                i++;
                            }
                        }
                    }
                    else if (a == "100000001")
                    { //Hợp đồng thế chấp
                        Entity temp = null;
                        StringBuilder str = new StringBuilder();
                        str.Append("HDTC");
                        traceService.Trace("HDTC");

                        traceService.Trace("the chap");
                        Hd.LogicalName = "new_hopdongthechap";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_chuhopdong"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                                str.Append("-" + temp["fullname"].ToString());

                            if (temp.Contains("new_socmnd"))
                                str.Append("-" + temp["new_socmnd"].ToString());
                        }
                        else
                        {
                            Hd["new_chuhopdongdoanhnghiep"] = Opp["customerid"];
                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                                str.Append("-" + temp["name"].ToString());

                            if (temp.Contains("new_sogpkd"))
                                str.Append("-" + temp["new_sogpkd"].ToString() + "- ");
                        }

                        Hdid = service.Create(Hd);
                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet("new_name"));

                        if (Hd.Contains("new_masohopdong"))
                            str.Append("-" + temp["new_masohopdong"].ToString());

                        newHD["new_name"] = str.ToString();
                        service.Update(newHD);

                        List<Entity> lstCTCH_TSTC = RetrieveMultiRecord(service, "new_chitietcohoi_taisanthechap",
                            new ColumnSet(true), "new_cohoi", Opp.Id);

                        foreach (Entity en in lstCTCH_TSTC)
                        {
                            Entity tstc = new Entity("new_taisanthechap");
                            tstc["new_giatridinhgiagiatrithechap"] = en.Contains("new_giatritstc") ? en["new_giatritstc"] : new decimal(0);
                            tstc["new_hopdongthechap"] = new EntityReference(Hd.LogicalName, Hdid);

                            service.Create(tstc);
                        }
                    }
                    else if (a == "100000003")
                    {//Hợp đồng đầu tư trang thiết bị
                        Entity temp = null;
                        StringBuilder str = new StringBuilder();
                        str.Append("HDTB");

                        traceService.Trace("trang thiet bi");
                        Hd.LogicalName = "new_hopdongdaututrangthietbi";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitaccungcap"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                            {
                                str.Append("-" + temp["fullname"].ToString());
                            }

                            if (temp.Contains("new_socmnd"))
                            {
                                str.Append("-" + temp["new_socmnd"].ToString());
                            }
                        }
                        else
                        {
                            Hd["new_doitaccungcapkhdn"] = Opp["customerid"];
                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                            {
                                str.Append("-" + temp["name"].ToString());
                            }

                            if (temp.Contains("new_sogpkd"))
                            {
                                str.Append("-" + temp["new_sogpkd"].ToString());
                            }
                        }
                        Hdid = service.Create(Hd);
                        traceService.Trace("Tạo được hđ");

                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet(new string[] { "new_name" }));
                        if (Hd.Contains("new_masohopdong"))
                        {
                            str.Append("-" + temp["new_masohopdong"].ToString());
                        }

                        newHD["new_name"] = str.ToString();

                        service.Update(newHD);
                        traceService.Trace("Update được HĐ");

                        List<Entity> lstCTCH_MMTB = RetrieveMultiRecord(service, "new_chitietcohoi_mmtb",
                            new ColumnSet(true), "new_cohoi", Opp.Id);

                        foreach (Entity en in lstCTCH_MMTB)
                        {
                            traceService.Trace("Vòng lặp tạo chi tiết cơ hội MMTB");
                            Entity CTHDDTTTB = new Entity("new_hopdongdaututrangthietbichitiet");
                            CTHDDTTTB["new_giatrithietbi"] = en.Contains("new_thanhtien") ? en["new_thanhtien"] : new decimal(0);
                            CTHDDTTTB["new_hopdongdaututrangthietbi"] = new EntityReference("new_hopdongdaututrangthietbi", Hdid);

                            service.Create(CTHDDTTTB);
                        }
                    }
                    else if (a == "100000004")
                    { //Hợp đồng thu hoạch
                        traceService.Trace("thu hoach");
                        Entity temp = null;
                        StringBuilder str = new StringBuilder();
                        str.Append("HDTH");

                        Hd.LogicalName = "new_hopdongthuhoach";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitacthuhoach"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                            {
                                str.Append("-" + temp["fullname"].ToString());
                            }

                            if (temp.Contains("new_socmnd"))
                            {
                                str.Append("-" + temp["new_socmnd"].ToString());
                            }
                        }
                        else
                        {
                            Hd["new_doitacthuhoachkhdn"] = Opp["customerid"];
                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                            {
                                str.Append("-" + temp["name"].ToString());
                            }

                            if (temp.Contains("new_sogpkd"))
                            {
                                str.Append("-" + temp["new_sogpkd"].ToString());
                            }
                        }
                        Hdid = service.Create(Hd);
                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet("new_name"));

                        if (Hd.Contains("new_masohopdong"))
                        {
                            str.Append("-" + temp["new_masohopdong"].ToString());
                        }
                        newHD["new_name"] = str.ToString();

                        service.Update(newHD);
                    }
                    else if (a == "100000005")
                    { //Hợp đồng vận chuyển
                        Entity temp = null;
                        StringBuilder str = new StringBuilder();
                        str.Append("HDVC");

                        traceService.Trace("van chuyen");
                        Hd.LogicalName = "new_hopdongvanchuyen";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitacvanchuyen"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                            {
                                str.Append("-" + temp["fullname"].ToString());
                            }

                            if (temp.Contains("new_socmnd"))
                            {
                                str.Append("-" + temp["new_socmnd"].ToString());
                            }
                        }
                        else
                        {
                            Hd["new_doitacvanchuyenkhdn"] = Opp["customerid"];
                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                            {
                                str.Append("-" + temp["name"].ToString());
                            }

                            if (temp.Contains("new_sogpkd"))
                            {
                                str.Append("-" + temp["new_sogpkd"].ToString());
                            }
                        }
                        Hdid = service.Create(Hd);
                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet("new_name"));
                        if (Hd.Contains("new_masohopdong"))
                        {
                            str.Append("-" + temp["new_masohopdong"].ToString());
                        }
                        newHD["new_name"] = str.ToString();
                        service.Update(newHD);

                    }
                    else if (a == "100000006")
                    { //Hợp đồng cung ứng dịch vụ
                        traceService.Trace("cung ung dich vu");

                        Entity temp = null;
                        StringBuilder str = new StringBuilder();
                        str.Append("HDDV");

                        Hd.LogicalName = "new_hopdongcungungdichvu";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_doitaccungcap"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                            {
                                str.Append("-" + temp["fullname"].ToString());
                            }

                            if (temp.Contains("new_socmnd"))
                            {
                                str.Append("-" + temp["new_socmnd"].ToString());
                            }
                        }
                        else
                        {
                            Hd["new_doitaccungcapkhdn"] = Opp["customerid"];

                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                            {
                                str.Append("-" + temp["name"].ToString());
                            }

                            if (temp.Contains("new_sogpkd"))
                            {
                                str.Append("-" + temp["new_sogpkd"].ToString());
                            }
                        }
                        Hd["new_loaicungcap"] = Opp["new_loaicungcap"];

                        Hdid = service.Create(Hd);
                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet("new_name"));

                        if (Hd.Contains("new_masohopdong"))
                        {
                            str.Append("-" + temp["new_masohopdong"].ToString());
                        }

                        newHD["new_name"] = str.ToString();
                        service.Update(newHD);
                    }
                    else if (a == "100000007")
                    { //Hợp đồng mua mía ngoài
                        traceService.Trace("mia ngoai");

                        Entity temp = null;
                        StringBuilder str = new StringBuilder();
                        str.Append("HDMB- ");

                        Hd.LogicalName = "new_hopdongmuabanmiangoai";
                        if (((EntityReference)Opp["customerid"]).LogicalName == "contact")
                        {
                            Hd["new_khachhang"] = Opp["customerid"];
                            temp = service.Retrieve("contact", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                            if (temp.Contains("fullname"))
                            {
                                str.Append("-" + temp["fullname"].ToString());
                            }

                            if (temp.Contains("new_socmnd"))
                            {
                                str.Append("-" + temp["new_socmnd"].ToString());
                            }
                        }
                        else
                        {
                            Hd["new_khachhangdoanhnghiep"] = Opp["customerid"];
                            temp = service.Retrieve("account", ((EntityReference)Opp["customerid"]).Id,
                                new ColumnSet(new string[] { "new_sogpkd", "name" }));

                            if (temp.Contains("name"))
                            {
                                str.Append("-" + temp["name"].ToString());
                            }

                            if (temp.Contains("new_sogpkd"))
                            {
                                str.Append("-" + temp["new_sogpkd"].ToString());
                            }
                        }
                        Hdid = service.Create(Hd);
                        Entity newHD = service.Retrieve(Hd.LogicalName, Hdid, new ColumnSet("new_name"));

                        if (Hd.Contains("new_masohopdong"))
                        {
                            str.Append("-" + temp["new_masohopdong"].ToString());
                        }

                        newHD["new_name"] = str.ToString();
                        service.Update(newHD);
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

        Entity GetThuadatFromMasoThua(Entity dndt, string masothua)
        {
            List<Entity> lstDenghidautumia = RetrieveMultiRecord(service, "new_chitietcohoi_dientichhopdong",
                new ColumnSet(new string[] { "new_thuadatmoi", "new_sothua" }), "new_cohoi", dndt.Id);
            Entity result = null;

            foreach (Entity a in lstDenghidautumia)
            {
                if (!a.Contains("new_sothua"))
                    continue;

                if ((string)a["new_sothua"] == masothua)
                    result = a;
            }

            Entity t = service.Retrieve(result.LogicalName, result.Id, new ColumnSet(new string[] { "new_thuadatmoi" }));
            string temp = t.Contains("new_thuadatmoi") ? (string)t["new_thuadatmoi"] : "";
            if (temp == "") throw new Exception("Mã số thửa không tồn tại !!");

            QueryExpression q = new QueryExpression("new_thuadat");
            q.ColumnSet = new ColumnSet(new string[] { "new_thuadatid" });
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_name", ConditionOperator.Equal, temp));

            EntityCollection entc = service.RetrieveMultiple(q);
            return entc[0];

        }

        Entity GetNKHBac()
        {
            QueryExpression q = new QueryExpression("new_nhomkhachhang");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_ma", ConditionOperator.Equal, "BAC"));

            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.FirstOrDefault();
        }
    }
}
