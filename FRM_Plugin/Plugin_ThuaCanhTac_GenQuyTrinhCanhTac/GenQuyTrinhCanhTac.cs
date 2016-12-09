using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Plugin_ThuaCanhTac_GenQuyTrinhCanhTac
{
    public class GenQuyTrinhCanhTac : IPlugin
    {
        private IOrganizationServiceFactory factory;
        private IOrganizationService service;
        ITracingService trace;

        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            Entity target = context.InputParameters["Target"] as Entity;
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.Depth == 1)
            {
                return;
            }

            if (target.LogicalName == "new_thuadatcanhtac")
            {
                if (context.MessageName == "Create")
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    Gen(target);
                }
                else if (context.MessageName == "Update")
                {
                    bool flag = false;

                    string[] arr = new string[] { "new_vutrong", "new_hopdongdautumia", "new_loaigocmia", "new_giongmia", "new_luugoc", "new_tuoimia", "new_mucdichsanxuatmia", "new_thuadat" };

                    foreach (string temp in arr)
                    {
                        if (target.Contains(temp))
                        {
                            flag = true;
                            break;
                        }
                    }


                    if (flag == true)
                    {
                        factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                        service = factory.CreateOrganizationService(context.UserId);
                        Entity chitiethddtm = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                        Gen(chitiethddtm);
                    }
                }
            }
        }

        private void Gen(Entity target)
        {
            if (!target.Attributes.Contains("new_hopdongdautumia"))
                throw new Exception("Vui lòng chọn hợp đồng đầu tư mía!");
            EntityReference new_hopdongdautumia = (EntityReference)target["new_hopdongdautumia"];
            Entity hopdongdautumia = service.Retrieve(
                new_hopdongdautumia.LogicalName,
                new_hopdongdautumia.Id,
                new ColumnSet(new string[]{
                        "new_vudautu",
                        "new_khachhang",
                        "new_khachhangdoanhnghiep",
                        "new_tram",
                        "new_canbonongvu",
                    }));
            if (hopdongdautumia == null)
                throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa", new_hopdongdautumia.Name));
            if (!hopdongdautumia.Attributes.Contains("new_vudautu"))
                throw new Exception(string.Format("Vui lòng chọn mùa vụ trong hợp đồng đầu tư mía '{0}'", new_hopdongdautumia.Name));
            EntityReference new_vudautu = (EntityReference)hopdongdautumia["new_vudautu"];
            EntityReference new_khachhang = null;
            string cmnd_gpkd = "";
            string mathua = "";
            string tenkh = "";

            if (!target.Attributes.Contains("new_khachhang"))
            {
                if (!target.Attributes.Contains("new_khachhangdoanhnghiep"))
                    throw new Exception("Vui lòng chọn khách hàng!");
                else
                {
                    if (!hopdongdautumia.Attributes.Contains("new_khachhangdoanhnghiep"))
                    {
                        throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'", new_hopdongdautumia.Name));
                    }
                    else if (((EntityReference)target["new_khachhangdoanhnghiep"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"]).Id.ToString())
                    {
                        throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên hợp đồng đầu tư chi tiết không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                    }
                    else
                    {
                        new_khachhang = (EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"];
                        Entity khdn = service.Retrieve(new_khachhang.LogicalName, new_khachhang.Id, new ColumnSet(new string[] { "new_sogpkd", "name" }));

                        if (!khdn.Contains("name"))
                        {
                            throw new Exception("Khách hàng doanh nghiệp không có tên !!!");
                        }
                        if (!khdn.Contains("new_sogpkd"))
                        {
                            throw new Exception(khdn["name"].ToString() + "không có số giấy phép kinh doanh !!!");
                        }

                        cmnd_gpkd = khdn["new_sogpkd"].ToString();
                        tenkh = khdn["name"].ToString();
                    }
                }
            }
            else
            {
                if (!hopdongdautumia.Attributes.Contains("new_khachhang"))
                {
                    throw new Exception(string.Format("Vui lòng chọn khách hàng trên hợp đồng đầu tư '{0}'!", new_hopdongdautumia.Name));
                }
                else if (((EntityReference)target["new_khachhang"]).Id.ToString() != ((EntityReference)hopdongdautumia["new_khachhang"]).Id.ToString())
                {
                    throw new Exception(string.Format("Khách hàng trên hợp đồng đầu tư mía '{0}' và khách hàng trên thửa canh tác không khớp. Vui lòng kiểm tra lại!", new_hopdongdautumia.Name));
                }
                else
                {

                    new_khachhang = (EntityReference)hopdongdautumia["new_khachhang"];
                    Entity khachhang = service.Retrieve(new_khachhang.LogicalName, new_khachhang.Id, new ColumnSet(new string[] { "new_socmnd", "fullname" }));

                    if (!khachhang.Contains("fullname"))
                    {
                        throw new Exception("Khách hàng không có tên !!!");
                    }
                    if (!khachhang.Contains("new_socmnd"))
                    {
                        throw new Exception(khachhang["fullname"].ToString() + "không có số chứng minh nhân dân!!!");
                    }

                    cmnd_gpkd = khachhang["new_socmnd"].ToString();
                    tenkh = khachhang["fullname"].ToString();
                }
            }

            if (!target.Attributes.Contains("new_thuadat"))
                throw new Exception("Vui lòng chọn thửa đất!");
            EntityReference new_thuadat = (EntityReference)target["new_thuadat"];
            Entity thuadat = service.Retrieve(new_thuadat.LogicalName, new_thuadat.Id, new ColumnSet(new string[]{
                    "new_nhomdat","new_loaidat","new_name"
                }));

            if (thuadat == null)
                throw new Exception(string.Format("Thửa đất '{0}' không tồn tại hoặc đã bị xóa!", new_thuadat.Name));
            if (!thuadat.Attributes.Contains("new_nhomdat"))
                throw new Exception(string.Format("Vui lòng chọn nhóm đất tại thửa đất '{0}", new_thuadat.Name));
            string new_nhomdat = ((OptionSetValue)thuadat["new_nhomdat"]).Value.ToString();

            if (!thuadat.Contains("new_name"))
            {
                throw new Exception("Thửa đất " + new_thuadat.Name + " chưa có mã thửa !!!");
            }
            mathua = thuadat["new_name"].ToString();
            if (!thuadat.Attributes.Contains("new_loaidat"))
            {
                throw new Exception(string.Format("Vui lòng chọn loại đất tại thửa đất '{0}'", new_thuadat.Name));
            }

            string new_loaidat = ((OptionSetValue)thuadat["new_loaidat"]).Value.ToString();

            if (!target.Attributes.Contains("new_vutrong"))
                throw new Exception("Vui lòng chọn vụ trồng!");
            int new_vutrong = ((OptionSetValue)target["new_vutrong"]).Value;

            //here
            if (!target.Attributes.Contains("new_loaigocmia"))
                throw new Exception("Vui lòng chọn loại gốc mía!");

            string new_loaigocmia = ((OptionSetValue)target["new_loaigocmia"]).Value.ToString();

            if (!target.Attributes.Contains("new_giongmia"))
                throw new Exception("Vui lòng chọn giống mía!");
            EntityReference new_giongmia = (EntityReference)target["new_giongmia"];

            //if (!target.Contains("new_luugoc"))
            //    throw new Exception("Vui lòng chọn lưu gốc!");

            //int new_luugoc = ((OptionSetValue)target["new_luugoc"]).Value;

            if (!target.Contains("new_tuoimia"))
            {
                throw new Exception("Vui lòng chọn tưới mía !!!");
            }

            bool tuoimia = (bool)target["new_tuoimia"];
            int tuoimia1 = (tuoimia == true ? 100000000 : 100000001);

            if (!target.Contains("new_mucdichsanxuatmia"))
                throw new Exception("Vui lòng chọn mục đích sản xuất!");
            string new_mucdichsanxuatmia = ((OptionSetValue)target["new_mucdichsanxuatmia"]).Value.ToString();

            Entity giongmia = service.Retrieve(new_giongmia.LogicalName, new_giongmia.Id, new ColumnSet(new string[] {
                    "new_vutrong",
                    //"new_loaigocmia",
                    "new_tuoichinmiagoc",
                    "new_khuyencaodattrong","new_nhomgiong","new_tuoichinmiato"}));

            if (giongmia == null)
                throw new Exception(string.Format("Giống mía '{0}' không tồn tại hoặc đã bị xóa!", new_giongmia.Name));
            if (!giongmia.Attributes.Contains("new_nhomgiong"))
                throw new Exception(string.Format("Vui Lòng chọn nhóm giống tại giống mía '{0}'!", new_giongmia.Name));
            string nhomgiong = ((OptionSetValue)giongmia["new_nhomgiong"]).Value.ToString();
            //throw new Exception("dsadsadsa");
            //if (!giongmia.Attributes.Contains("new_tuoichinmiato"))
            //    throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía tơ' tại giống mia '{0}'!", new_giongmia.Name));
            //int tuoichinmiato = (int)giongmia["new_tuoichinmiato"];

            //if (!giongmia.Attributes.Contains("new_tuoichinmiagoc"))
            //    throw new Exception(string.Format("Vui lòng nhập 'tuổi chín mía gốc' tại giống mia '{0}'!", new_giongmia.Name));
            //int tuoichinmiagoc = (int)giongmia["new_tuoichinmiagoc"];

            if (!target.Attributes.Contains("new_ngaytrongdukien"))
                throw new Exception("Vui lòng chọn ngày trồng dự kiến!");
            DateTime new_ngaytrong = (DateTime)target["new_ngaytrongdukien"];

            QueryExpression q = new QueryExpression("new_quitrinhcanhtac");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression(LogicalOperator.And);
            //LinkEntity linkgiongmia = new LinkEntity("new_quitrinhcanhtac", "new_new_quitrinhcanhtac_new_giongmia", "new_quitrinhcanhtacid", "new_quitrinhcanhtacid", JoinOperator.Inner);

            //q.LinkEntities.Add(linkgiongmia);
            //linkgiongmia.LinkCriteria = new FilterExpression();
            //linkgiongmia.LinkCriteria.AddCondition("new_giongmiaid", ConditionOperator.Equal, giongmia.Id);
            //throw new Exception("Tuoi mia: " + tuoimia1 + " nhom giong mia : " + nhomgiong.ToString() + "Nhom dat: " + new_nhomdat + "loai dat : "
            //+ new_loaidat + "Vu trong : " + new_vutrong.ToString() + "Muc dich san xuat mia : " + new_mucdichsanxuatmia + "goc mia : " + new_loaigocmia);

            //FilterExpression filtertuoimia = new FilterExpression(LogicalOperator.Or);
            //filtertuoimia.AddCondition(new ConditionExpression("new_hidetuoimia", ConditionOperator.DoesNotContain));
            //filtertuoimia.AddCondition(new ConditionExpression("new_hidetuoimia", ConditionOperator.Like, "%" + tuoimia1 + "%"));
            //q.Criteria.AddFilter(filtertuoimia);

            //FilterExpression filtergiongmia = new FilterExpression(LogicalOperator.Or);
            //filtergiongmia.AddCondition(new ConditionExpression("new_hidenhomgiongmia", ConditionOperator.DoesNotContain));
            //filtergiongmia.AddCondition(new ConditionExpression("new_hidenhomgiongmia", ConditionOperator.Like, "%" + nhomgiong + "%"));
            //q.Criteria.AddFilter(filtergiongmia);

            //FilterExpression filternhomdat = new FilterExpression(LogicalOperator.Or);
            //filternhomdat.AddCondition(new ConditionExpression("new_hidenhomdat", ConditionOperator.DoesNotContain));
            //filternhomdat.AddCondition(new ConditionExpression("new_hidenhomdat", ConditionOperator.Like, "%" + new_nhomdat + "%"));
            //q.Criteria.AddFilter(filternhomdat);

            //FilterExpression filterloaidat = new FilterExpression(LogicalOperator.Or);
            //filterloaidat.AddCondition(new ConditionExpression("new_loaidat_vl", ConditionOperator.DoesNotContain));
            //filterloaidat.AddCondition(new ConditionExpression("new_loaidat_vl", ConditionOperator.Like, "%" + new_loaidat + "%"));
            //q.Criteria.AddFilter(filterloaidat);

            //FilterExpression filtervutrong = new FilterExpression(LogicalOperator.Or);
            //filtervutrong.AddCondition(new ConditionExpression("new_hidevutrong", ConditionOperator.DoesNotContain));
            //filtervutrong.AddCondition(new ConditionExpression("new_hidevutrong", ConditionOperator.Like, "%" + new_vutrong.ToString() + "%"));
            //q.Criteria.AddFilter(filtervutrong);

            //FilterExpression filtermucdichsanxuatmia = new FilterExpression(LogicalOperator.Or);
            //filtermucdichsanxuatmia.AddCondition(new ConditionExpression("new_hidemucdichsanxuatmia", ConditionOperator.DoesNotContain));
            //filtermucdichsanxuatmia.AddCondition(new ConditionExpression("new_hidemucdichsanxuatmia", ConditionOperator.Like, "%" + new_mucdichsanxuatmia + "%"));
            //q.Criteria.AddFilter(filtermucdichsanxuatmia);

            //FilterExpression filterloaigocmia = new FilterExpression(LogicalOperator.Or);
            //filterloaigocmia.AddCondition(new ConditionExpression("new_hideloaigocmia", ConditionOperator.DoesNotContain));
            //filterloaigocmia.AddCondition(new ConditionExpression("new_hideloaigocmia", ConditionOperator.Like, "%" + new_loaigocmia + "%"));
            //q.Criteria.AddFilter(filterloaigocmia);

            StringBuilder fetchXML = new StringBuilder();
            fetchXML.AppendFormat("<fetch mapping='logical' version='1.0'>");
            fetchXML.AppendFormat("<entity name='new_quitrinhcanhtac'>");
            fetchXML.AppendFormat("<all-attributes />");
            fetchXML.AppendFormat("<filter type='and'>");
            fetchXML.AppendFormat("<filter type='or'>");
            fetchXML.AppendFormat("<condition attribute='new_hidetuoimia' operator='like' value='%{0}%' />", tuoimia1);
            fetchXML.AppendFormat("<condition attribute='new_hidetuoimia' operator='null' />");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("<filter type='or'>");
            fetchXML.AppendFormat("<condition attribute='new_hidenhomgiongmia' operator='like' value='%{0}%' />", nhomgiong);
            fetchXML.AppendFormat("<condition attribute='new_hidenhomgiongmia' operator='null' />");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("<filter type='or'>");
            fetchXML.AppendFormat("<condition attribute='new_hidenhomdat' operator='like' value='%{0}%' />", new_nhomdat);
            fetchXML.AppendFormat("<condition attribute='new_hidenhomdat' operator='null' />");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("<filter type='or'>");
            fetchXML.AppendFormat("<condition attribute='new_loaidat_vl' operator='like' value='%{0}%' />", new_loaidat);
            fetchXML.AppendFormat("<condition attribute='new_loaidat_vl' operator='null' />");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("<filter type='or'>");
            fetchXML.AppendFormat("<condition attribute='new_hidevutrong' operator='like' value='%{0}%' />", new_vutrong);
            fetchXML.AppendFormat("<condition attribute='new_hidevutrong' operator='null' />");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("<filter type='or'>");
            fetchXML.AppendFormat("<condition attribute='new_hidemucdichsanxuatmia' operator='like' value='%{0}%' />", new_mucdichsanxuatmia);
            fetchXML.AppendFormat("<condition attribute='new_hidemucdichsanxuatmia' operator='null' />");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("<filter type='or'>");
            fetchXML.AppendFormat("<condition attribute='new_hideloaigocmia' operator='like' value='%{0}%' />", new_loaigocmia);
            fetchXML.AppendFormat("<condition attribute='new_hideloaigocmia' operator='null' />");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("</filter>");
            fetchXML.AppendFormat("</entity>");
            fetchXML.AppendFormat("</fetch>");

            //throw new Exception(fetchXML.ToString());
            q.TopCount = 1;
            Entity quytrinhcanhtac = null;
            EntityCollection entc = service.RetrieveMultiple(new FetchExpression(fetchXML.ToString()));

            for (int i = 0; i < entc.Entities.Count; i++)
            {
                Entity a = entc[i];
                QueryExpression query = new QueryExpression("new_new_quitrinhcanhtac_new_vudautu");
                query.ColumnSet = new ColumnSet(new string[] { "new_new_quitrinhcanhtac_new_vudautuid", "new_vudautuid" });
                query.Criteria.AddCondition(new ConditionExpression("new_quitrinhcanhtacid", ConditionOperator.Equal, a.Id));

                EntityCollection vudautus = service.RetrieveMultiple(query);
                if (vudautus.Entities.Count() <= 0)
                    continue;
                else
                {
                    bool co1 = false;
                    foreach (Entity b in vudautus.Entities)
                    {
                        if (((EntityReference)b["new_vudautuid"]).Id == new_vudautu.Id)
                        {
                            co1 = true;
                            break;
                        }
                    }
                    if (co1 == false)
                    {
                        entc.Entities.RemoveAt(i);
                        i--;
                    }
                }
            }
            if (entc.Entities.Count > 0)
            {
                quytrinhcanhtac = entc.Entities[0];
                Entity a = new Entity(target.LogicalName);
                a.Id = target.Id;
                a["new_quitrinhcanhtac"] = quytrinhcanhtac.ToEntityReference();
                service.Update(a);
            }
            else throw new Exception(string.Format("Vụ đầu tư '{0}' chưa có quy trình canh tác '{1}'!", new_vudautu.Name, quytrinhcanhtac["new_name"]));
            if (quytrinhcanhtac != null)
            {
                QueryExpression q1 = new QueryExpression("new_quitrinhcanhtacchitiet");
                q1.ColumnSet = new ColumnSet(new string[] {
                    "new_name",
                    "new_hangmuccanhtac",
                    "new_songaysaukhitrong",
                    "new_quitrinhcanhtac",
                    "new_sothoigianthuchien",
                    "new_lanthuchien"
                });
                q1.Orders.Add(new OrderExpression("new_songaysaukhitrong", OrderType.Ascending));
                q1.Criteria = new FilterExpression(LogicalOperator.And);
                q1.Criteria.AddCondition(new ConditionExpression("new_quitrinhcanhtac", ConditionOperator.Equal, quytrinhcanhtac.Id));
                EntityCollection qtcs = service.RetrieveMultiple(q1);
                if (qtcs.Entities.Count() <= 0)
                    throw new Exception(string.Format("Quy trình canh tác '{0}' chưa có quy trình canh tác chi tiết. Vui lòng thêm quy trình canh tác chi tiết!", quytrinhcanhtac["new_name"].ToString()));

                foreach (Entity qtc in qtcs.Entities)
                {
                    string qtctctName = qtc.Attributes.Contains("new_name") ? "'" + qtc["new_name"] + "'" : "";
                    if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                        throw new Exception(string.Format("Vui lòng chọn hạng mục canh tác trên Quy trình canh tác chi tiết {0}!", qtctctName));
                    EntityReference hmRef = qtc["new_hangmuccanhtac"] as EntityReference;
                    Entity hm = service.Retrieve(hmRef.LogicalName, hmRef.Id, new ColumnSet(new string[] { "new_name", "new_loaihangmuc", "new_yeucau" }));
                    if (hm == null)
                        throw new Exception(string.Format("Hạng mục canh tác '{0}' trên quy trình canh tác chi tiết '{1}' không tồ tại hoặc bị xóa!", hmRef.Name, qtctctName));
                    if (!hm.Attributes.Contains("new_loaihangmuc"))
                        throw new Exception(string.Format("Vui lòng chọn loại hạng mục canh tác trên hạng muc canh tác '{0}'!", hmRef.Name));
                    int type = ((OptionSetValue)hm["new_loaihangmuc"]).Value;
                    //trace.Trace("1");
                    switch (type)
                    {
                        case 100000001://Trồng mía
                            {
                                //trace.Trace("2");
                                Entity en = new Entity("new_trongmia");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                en["subject"] = hm["new_name"] + "-" + tenkh + "-" + mathua + "-" + cmnd_gpkd;
                                en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongtrongmia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                en["new_hangmucanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                //en["new_lanbon"] = qtc["new_lanthuchien"];
                                en["new_vutrong"] = new OptionSetValue(new_vutrong);
                                en["new_tram"] = hopdongdautumia["new_tram"];
                                en["new_canbonongvu"] = hopdongdautumia["new_canbonongvu"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình '{1}'", qtctctName, quytrinhcanhtac["new_name"]));
                                en["scheduledstart"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    en["new_ngaytrongxulygoc"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);

                            }
                            break;
                        case 100000002://Bón phân
                            {
                                //trace.Trace("3");
                                Entity bp = new Entity("new_bonphan");

                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                bp["subject"] = hm["new_name"] + "-" + tenkh + "-" + mathua + "-" + cmnd_gpkd;
                                bp["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                bp["new_hopdongtrongmia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                bp["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                bp["new_lanbon"] = qtc["new_lanthuchien"];
                                bp["new_tram"] = hopdongdautumia["new_tram"];
                                bp["new_canbonongvu"] = hopdongdautumia["new_canbonongvu"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName,quytrinhcanhtac["new_name"]));
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                bp["scheduledstart"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    bp["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                                bp["regardingobjectid"] = target.ToEntityReference();
                                service.Create(bp);

                            }
                            break;
                        case 100000003://Xử lý cỏ dại
                            {
                                //new_xulycodai
                                Entity en = new Entity("new_xulycodai");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập Tên QT canh tác chi tiết của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                en["subject"] = hm["new_name"] + "-" + tenkh + "-" + mathua + "-" + cmnd_gpkd;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];

                                en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongtrongmia"] = new_hopdongdautumia;
                                en["new_tram"] = hopdongdautumia["new_tram"];
                                en["new_canbonongvu"] = hopdongdautumia["new_canbonongvu"];

                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_lanxuly"] = qtc["new_lanthuchien"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["scheduledstart"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);

                            }
                            break;
                        case 100000006://Tưới mía
                            {
                                //trace.Trace("5");
                                Entity en = new Entity("new_tuoimia");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["subject"] = hm["new_name"] + "-" + tenkh + "-" + mathua + "-" + cmnd_gpkd;
                                en["new_thuacanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                en["new_hopdongtrongmia"] = new_hopdongdautumia;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                if (!qtc.Attributes.Contains("new_lanthuchien"))
                                    throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["new_solantuoi"] = qtc["new_lanthuchien"];
                                en["new_tram"] = hopdongdautumia["new_tram"];
                                en["new_canbonongvu"] = hopdongdautumia["new_canbonongvu"];

                                //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                en["scheduledstart"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    en["new_thoigiantuoigiotrenngay"] = qtc["new_sothoigianthuchien"];
                                en["regardingobjectid"] = target.ToEntityReference();
                                service.Create(en);

                            }
                            break;
                        case 100000004: //Xử lý sâu bệnh 
                            {
                                {
                                    Entity en = new Entity("new_xulysaubenh");
                                    if (!qtc.Attributes.Contains("new_name"))
                                        throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}' của quy trình canh tac '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                    en["subject"] = hm["new_name"] + "-" + tenkh + "-" + mathua + "-" + cmnd_gpkd;
                                    en["new_chitiethddtmia"] = new EntityReference(target.LogicalName, target.Id);
                                    en["new_hopdongdautumia"] = new_hopdongdautumia;
                                    if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                        throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                    en["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                    if (!qtc.Attributes.Contains("new_lanthuchien"))
                                        throw new Exception(string.Format("Vui lòng nhập số lần thực hiện trên quy trình  canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));
                                    en["new_lanxuly"] = qtc["new_lanthuchien"];
                                    en["new_tram"] = hopdongdautumia["new_tram"];
                                    en["new_canbonongvu"] = hopdongdautumia["new_canbonongvu"];
                                    //if (!qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    //    throw new Exception(string.Format("Vui lòng nhập số ngày thực hiện trên quy trình  canh tác chi tiết '{0}'!", qtctctName));
                                    //en["new_songaythuchien"] = qtc["new_sothoigianthuchien"];

                                    if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                        throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình {1}!", qtctctName, quytrinhcanhtac["new_name"]));

                                    en["scheduledstart"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);

                                    //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                    //    en["new_ngayxulythucte"] = qtc["new_sothoigianthuchien"];
                                    en["regardingobjectid"] = target.ToEntityReference();
                                    service.Create(en);

                                }
                            }
                            break;
                        case 100000007:
                        case 100000005:
                        case 100000008:
                        case 100000009:
                        case 100000000:
                        default:
                            //100000007:Khai mương chống úng" Khai mương chống úng
                            //100000007:Bóc lột lá mía || 100000008:khach || 100000009:San lấp mặt bằng
                            //100000000:Cày
                            {
                                //trace.Trace("7");
                                Entity nk = new Entity("new_nhatkydongruong");
                                if (!qtc.Attributes.Contains("new_name"))
                                    throw new Exception(string.Format("Vui lòng nhập 'Ten QT canh tác chi tiết' của quy trình canh tác '{0}'", quytrinhcanhtac["new_name"]));
                                nk["subject"] = hm["new_name"] + "-" + tenkh + "-" + mathua + "-" + cmnd_gpkd;
                                if (!qtc.Attributes.Contains("new_hangmuccanhtac"))
                                    throw new Exception(string.Format("Vui lòng nhập hạng mục canh tác trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                nk["new_hangmuccanhtac"] = qtc["new_hangmuccanhtac"];
                                nk["new_hopdongdautumia"] = new_hopdongdautumia;
                                nk["new_tram"] = hopdongdautumia["new_tram"];
                                nk["new_canbonongvu"] = hopdongdautumia["new_canbonongvu"];
                                nk["new_thuadatcanhtac"] = new EntityReference(target.LogicalName, target.Id);
                                if (new_khachhang.LogicalName == "contact")
                                    nk["new_khachhang"] = new_khachhang;
                                else if (new_khachhang.LogicalName == "account")
                                    nk["new_khachhangdoanhnghiep"] = new_khachhang;
                                if (!qtc.Attributes.Contains("new_songaysaukhitrong"))
                                    throw new Exception(string.Format("Vui lòng nhập số ngày sau khi trồng trên QT canh tác chi tiết '{0}' của quy trình canh tác '{1}'!", qtctctName, quytrinhcanhtac["new_name"]));
                                nk["scheduledstart"] = new_ngaytrong.AddDays((int)qtc["new_songaysaukhitrong"]);
                                //if (qtc.Attributes.Contains("new_sothoigianthuchien"))
                                //    nk["new_songaythuchien"] = qtc["new_sothoigianthuchien"];
                                nk["regardingobjectid"] = target.ToEntityReference();
                                service.Create(nk);

                            }
                            break;
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
