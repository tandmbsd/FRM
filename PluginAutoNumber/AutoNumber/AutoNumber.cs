using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace AutoNumber
{
    public class AutoNumber : IPlugin
    {
        // moi nhat
        private IOrganizationServiceFactory serviceProxy;
        private IOrganizationService service;
        public ITracingService tracingService;
        private System.Threading.Mutex mtx = null;
        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            //ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.MessageName == "Create")
            {
                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is Entity)
                {
                    serviceProxy = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = serviceProxy.CreateOrganizationService(context.UserId);
                    Entity en = (Entity)context.InputParameters["Target"];
                    string mutexName = string.Format("{0}{1}", en.LogicalName, "Autonumber");
                    if (en.LogicalName == "bsd_autonumber")
                    {
                        if (en["bsd_entitylogical"].ToString() == "bsd_autonumber")
                            throw new Exception("Can not use bsd_autonumber entity!");
                        else if (IsExisted(en["bsd_entitylogical"].ToString(), en["bsd_fieldlogical"].ToString()))
                            throw new Exception("This rule has already existed!");
                    }
                    else
                    {
                        bool flag = false;
                        if (Count(en.LogicalName) > 0)
                        {
                            EntityCollection enc = this.RetrieveAutoNumbers(en.LogicalName);
                            foreach (Entity e in enc.Entities)
                            {
                                int length = e.Attributes.Contains("bsd_length") ? (int)e["bsd_length"] : 0;
                                if (length < 0)
                                    return;
                                bool isUseCustom = e.Attributes.Contains("bsd_usecustom") ? (bool)e["bsd_usecustom"] : false;
                                if (isUseCustom)
                                {
                                    string prefix = e.Attributes.Contains("bsd_prefix") ? e["bsd_prefix"].ToString() : string.Empty;
                                    string sufix = e.Attributes.Contains("bsd_sufix") ? e["bsd_sufix"].ToString() : string.Empty;

                                    ulong currentPos = e.Attributes.Contains("bsd_currentposition") ? ulong.Parse(e["bsd_currentposition"].ToString()) : 0;
                                    string field = e.Attributes.Contains("bsd_fieldlogical") ? e["bsd_fieldlogical"].ToString() : string.Empty;
                                    if (!string.IsNullOrWhiteSpace(field))
                                    {
                                        string middle = "";
                                        if (length > 0)
                                        {
                                            currentPos++;
                                            var crLength = length - currentPos.ToString().Length;
                                            if (crLength < 0)
                                            {
                                                currentPos = 1;
                                                crLength = length - 1;
                                            }

                                            for (int i = 0; i < crLength; i++)
                                                middle += "0";
                                            en[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                            e["bsd_currentposition"] = currentPos.ToString();
                                            service.Update(e);
                                        }
                                        else
                                            en[field] = string.Format("{0}{1}{2}{3}", prefix, middle, "", sufix);
                                        flag = true;
                                    }
                                }
                                else
                                {
                                    Update(ref flag, e, length, ref en);
                                }
                            }
                            //if (flag)
                            //    service.Update(en);
                        }
                    }
                }
            }
        }

        private void Update(ref bool flag, Entity eAu, int length, ref Entity eField)
        {
            switch (eField.LogicalName)
            {
                #region new_thuadat
                case "new_thuadat":
                    {
                        if (!eField.Attributes.Contains("new_vungdialy"))
                            throw new Exception("Vui lòng chọn vùng địa lý!");
                        EntityReference vdlRef = (EntityReference)eField["new_vungdialy"];
                        Entity vungDiaLy = service.Retrieve(vdlRef.LogicalName, vdlRef.Id,
                            new ColumnSet(new string[] { "new_mavungdialy", "new_matatvungdialy" }));

                        if (vungDiaLy == null)
                            throw new Exception("Vùng địa lý không tồn tại hoặc đã bị xóa. Vui lòng kiểm tra lại!");

                        string prefix = vungDiaLy.Attributes.Contains("new_matatvungdialy") ? vungDiaLy["new_matatvungdialy"].ToString() : "";
                        if (((OptionSetValue)eAu["bsd_grouptype"]).Value == 100000000) // global
                        {
                            string sufix = "";
                            ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                            string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }
                                string middle = "-";
                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                service.Update(eAu);
                            }
                        }
                        else // with detail
                        {
                            Entity Autonumberdetail = RetrieveBsdAutonumberdetail(prefix, eAu);

                            if (Autonumberdetail == null)
                            {
                                Entity en = new Entity("bsd_autonumberdetail");

                                string sufix = "";
                                ulong currentPos = 0;
                                string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                                if (!string.IsNullOrWhiteSpace(field))
                                {
                                    currentPos++;
                                    var crLength = length - currentPos.ToString().Length;
                                    if (crLength < 0)
                                    {
                                        currentPos = 1;
                                        crLength = length - 1;
                                    }
                                    string middle = "-";
                                    for (int i = 0; i < crLength; i++)
                                        middle += "0";

                                    eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                    eAu["bsd_currentposition"] = currentPos.ToString();
                                    flag = true;

                                    en["bsd_currentposition"] = currentPos + "";
                                    en["bsd_name"] = prefix;
                                    en["bsd_autonumberid"] = new EntityReference(eAu.LogicalName, eAu.Id);
                                    service.Create(en);
                                    service.Update(eAu);
                                }
                            }
                            else
                            {
                                string sufix = "";
                                ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                                string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                                if (!string.IsNullOrWhiteSpace(field))
                                {
                                    currentPos++;
                                    var crLength = length - currentPos.ToString().Length;
                                    if (crLength < 0)
                                    {
                                        currentPos = 1;
                                        crLength = length - 1;
                                    }
                                    string middle = "-";
                                    for (int i = 0; i < crLength; i++)
                                        middle += "0";

                                    eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                    eAu["bsd_currentposition"] = currentPos.ToString();
                                    flag = true;

                                    Autonumberdetail["bsd_currentposition"] = currentPos + "";

                                    service.Update(Autonumberdetail);
                                    service.Update(eAu);
                                }
                            }
                        }
                    }
                    break;
                #endregion
                #region new_vudautu
                case "new_vudautu":
                    {
                        if (!eField.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu!");
                        if (!eField.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc!");

                        DateTime begin_date = (DateTime)eField["new_ngaybatdau"];
                        DateTime en_date = (DateTime)eField["new_ngayketthuc"];
                        string prefix = begin_date.Year + "-" + en_date.Year;
                        //throw new Exception(begin_date.ToLongDateString() + "|" + en_date.ToLongDateString());
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            string middle = "";
                            if (length > 0)
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";
                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                eAu["bsd_currentposition"] = currentPos.ToString();
                                service.Update(eAu);
                            }
                            else
                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, "", sufix);
                            flag = true;
                        }
                    }
                    break;
                #endregion
                #region new_hopdongthuhoach
                case "new_hopdongthuhoach":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception("Vụ đầu tư không tồn tại hoặc bị xóa!");
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        int yDate = bDate.Year + 1;

                        string prefix = yDate + "TH";
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;

                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_banggiacongdon
                case "new_banggiacongdon":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = "/CD/" + String.Format("{0:yy}-{1:yy}", bDate, eDate);

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);

                            }
                        }
                        else
                        {
                            service.Update(Autonumberdetail);
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);

                            }
                        }
                    }
                    break;
                #endregion
                #region new_hopdongvanchuyen
                case "new_hopdongvanchuyen":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception("Vụ đầu tư không tồn tại hoặc bị xóa!");
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        int yDate = bDate.Year + 1;

                        string prefix = yDate + "VC";
                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_banggiavanchuyen
                case "new_banggiavanchuyen":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));
                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = "/VC/" + String.Format("{0:yy}-{1:yy}", bDate, eDate);

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_hopdongthechap
                case "new_hopdongthechap":
                    {
                        //if (!eField.Attributes.Contains("new_vudautu"))
                        //    throw new Exception("Vui lòng chọn vụ đầu tư!");
                        //EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        //Entity vdt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                        //new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        int year = DateTime.Now.Year;

                        //get key
                        string key = year.ToString();
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = "";
                        string sufix = String.Format("/TC/{0}/HĐ-TTCS", year);
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_chinhsachdautu
                case "new_chinhsachdautu":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = String.Format("/ĐT/{0:yy}-{1:yy}", begin, end);

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_hopdongdautumia
                case "new_hopdongdautumia":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception("Vụ đầu tư không tồn tại hoặc bị xóa!");
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = ((DateTime)dt["new_ngaybatdau"]);
                        int yDate = bDate.Year + 1;
                        string prefix = yDate + "DT";
                        string sufix = "";
                        string middle = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;
                            currentPos++;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;
                                Autonumberdetail["bsd_currentposition"] = currentPos + "";
                                service.Update(Autonumberdetail);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_hopdongthuedat
                case "new_hopdongthuedat":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception("Vụ đầu tư không tồn tại hoặc bị xóa!");
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        int yDate = bDate.Year + 1;

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = yDate + "TD";
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;

                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;

                                Autonumberdetail["bsd_currentposition"] = currentPos + "";

                                service.Update(Autonumberdetail);

                            }
                        }

                    }
                    break;
                #endregion
                #region new_hopdongdautuhatang
                case "new_hopdongdautuhatang":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];

                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception("Vụ đầu tư không tồn tại hoặc bị xóa!");
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        int yDate = bDate.Year + 1;

                        string prefix = yDate + "HT";
                        string sufix = "";
                        string middle = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (Autonumberdetail == null)
                        {

                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);

                                flag = true;

                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;

                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthudichvu
                case "new_nghiemthudichvu":
                    {
                        if (!eField.Attributes.Contains("new_hopdongdautumia"))
                            throw new Exception("Vui lòng chọn hợp đồng tư mía!");
                        EntityReference hdref = (EntityReference)eField["new_hopdongdautumia"];
                        Entity hd = service.Retrieve(hdref.LogicalName, hdref.Id,
                            new ColumnSet(new string[] { "new_vudautu" }));

                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!", hdref.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư mía '{0}'!", hdref.Name));

                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng đầu tư mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdref.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("NTDV{0:yy}{1:yy}-", begin, end);
                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthukhac
                case "new_nghiemthukhac":
                    {
                        if (!eField.Attributes.Contains("new_hopdongdautumia"))
                            throw new Exception("Vui lòng chọn hợp đồng tư mía!");
                        EntityReference hdref = (EntityReference)eField["new_hopdongdautumia"];
                        Entity hd = service.Retrieve(hdref.LogicalName, hdref.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!", hdref.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư mía '{0}'!", hdref.Name));

                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng đầu tư mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdref.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("NTKHAC{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthumaymocthietbi
                case "new_nghiemthumaymocthietbi":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = string.Format("R{0:yy}-{1:yy}NM", bDate, eDate);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }


                    }
                    break;
                #endregion
                #region new_nghiemthuthuedat
                case "new_nghiemthuthuedat":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = string.Format("NTTD{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieudenghigiaingan
                case "new_phieudenghigiaingan":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = string.Format("DNGN{0:yy}-{1:yy}", begin, end);
                        string sufix = "";
                        string middle = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieugiaonhanphanbon
                case "new_phieugiaonhanphanbon":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("GNPB{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieugiaonhanhomgiong
                case "new_phieugiaonhanhomgiong":
                    {
                        if (!eField.Attributes.Contains("new_hopdongdautumia"))
                            throw new Exception("Vui lòng chọn hợp đồng đầu tư mía!");
                        EntityReference hdRef = (EntityReference)eField["new_hopdongdautumia"];
                        Entity hd = service.Retrieve(hdRef.LogicalName, hdRef.Id,
                            new ColumnSet(new string[] { "new_vudautu" }));

                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!", hdRef.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư mía '{0}'!", hdRef.Name));
                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng đầu tư mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("GNHG{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                service.Update(eAu);

                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthutuoimia
                case "new_nghiemthutuoimia":
                    {
                        if (!eField.Attributes.Contains("new_hopdongtrongmia"))
                            throw new Exception("Vui lòng chọn hợp đồng trồng mía!");
                        EntityReference hdRef = (EntityReference)eField["new_hopdongtrongmia"];
                        Entity hd = service.Retrieve(hdRef.LogicalName, hdRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng trồng mía '{0}' không tồn tại hoặc bị xóa!", hdRef.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng trồng mía '{0}'!", hdRef.Name));
                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng trồng mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("NTTUOI{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);

                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthutrongmia
                case "new_nghiemthutrongmia":
                    {
                        if (!eField.Attributes.Contains("new_hopdongtrongmia"))
                            throw new Exception("Vui lòng chọn hợp đồng trồng mía!");
                        EntityReference hdRef = (EntityReference)eField["new_hopdongtrongmia"];
                        Entity hd = service.Retrieve(hdRef.LogicalName, hdRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng trồng mía '{0}' không tồn tại hoặc bị xóa!", hdRef.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng trồng mía '{0}'!", hdRef.Name));
                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng trồng mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("NTTM{0:yy}{1:yy}-", begin, end);
                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);

                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_hopdongdaututrangthietbi
                case "new_hopdongdaututrangthietbi":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)vdt["new_ngaybatdau"];
                        int yDate = bDate.Year + 1;

                        string prefix = yDate + "MTB";

                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                flag = true;

                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);

                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_hopdongcungungdichvu
                case "new_hopdongcungungdichvu":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        if (!eField.Attributes.Contains("new_loaicungcap"))
                            throw new Exception("Vui lòng chọn loại cung cấp");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)vdt["new_ngaybatdau"];
                        int yDate = bDate.Year + 1;

                        string prefix = yDate + "DV";
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_chinhsachthumua
                case "new_chinhsachthumua":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = String.Format("/TM/{0:yy}{1:yy}", begin, end);
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_lenhdon
                case "new_lenhdon":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        tracingService.Trace("1");
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));

                        if (dt == null)
                            throw new Exception("Vụ đầu tư không tồn tại hoặc bị xóa!");
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọn ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];
                        tracingService.Trace("2");
                        string prefix = string.Format("LD{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        tracingService.Trace("3");
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;

                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }

                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";
                            tracingService.Trace("4");
                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                            tracingService.Trace("5");
                        }
                    }
                    break;
                #endregion
                #region new_xevanchuyen
                case "new_xevanchuyen":
                    {
                        if (!eField.Attributes.Contains("new_loaixe"))
                            throw new Exception("Vui lòng chọn loại xe vận chuyển!");

                        int type = ((OptionSetValue)eField["new_loaixe"]).Value;
                        string prefix = "";

                        switch (type)
                        {
                            case 100000000: // xe tai
                                {
                                    prefix = "TAI";
                                }
                                break;
                            case 100000001: //kéo
                                {
                                    prefix = "MK";
                                }
                                break;
                            case 100000002: // container
                                {
                                    prefix = "CON";
                                }
                                break;
                        }


                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(prefix, eAu);

                        if (Autonumberdetail == null)
                        {
                            Entity en = new Entity("bsd_autonumberdetail");
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;

                                en["bsd_currentposition"] = currentPos + "";
                                en["bsd_name"] = prefix;
                                en["bsd_autonumberid"] = new EntityReference(eAu.LogicalName, eAu.Id);
                                service.Create(en);
                                service.Update(eAu);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;


                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;

                                Autonumberdetail["bsd_currentposition"] = currentPos + "";

                                service.Update(Autonumberdetail);
                                service.Update(eAu);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieudangtai
                case "new_phieudangtai":
                    {
                        //if (!eField.Attributes.Contains("new_lenhdon"))
                        //    throw new Exception("Vui lòng chọn lệnh đốn!");
                        //EntityReference ldRef = (EntityReference)eField["new_lenhdon"];
                        //Entity ldE = service.Retrieve(ldRef.LogicalName, ldRef.Id, new ColumnSet(new string[] { "new_vudautu", "new_xevanchuyen" }));
                        //if (ldE == null)
                        //    throw new Exception(string.Format("Lệnh đốn '{0}' không tồn tại hoặc bị xóa!", ldRef.Name));
                        //if (!ldE.Attributes.Contains("new_vudautu"))
                        //    throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên lệnh đốn '{0}'!", ldRef.Name));
                        //if (!ldE.Attributes.Contains("new_xevanchuyen"))
                        //    throw new Exception(string.Format("Vui lòng chọn chuyến xe trên lệnh đốn '{0}'!", ldRef.Name));

                        //EntityReference dtRef = (EntityReference)ldE["new_vudautu"];
                        //Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau" }));
                        //if (dt == null)
                        //    throw new Exception(string.Format("Vụ đầu tư '{0}' trên lệnh đốn '{1}' không tồn tại hoặc bị xóa!", dtRef.Name, ldRef.Name));
                        //if (!dt.Attributes.Contains("new_ngaybatdau"))
                        //    throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu vụ đầu tư '{0}' trên lệnh đốn '{1}'!", dtRef.Name, ldRef.Name));
                        //DateTime bDate = (DateTime)dt["new_ngaybatdau"];

                        //EntityReference cxRef = (EntityReference)ldE["new_xevanchuyen"];
                        //Entity cxE = service.Retrieve(cxRef.LogicalName, cxRef.Id, new ColumnSet(new string[] { "new_loaixe" }));
                        //if (!cxE.Contains("new_loaixe"))
                        //    throw new Exception(string.Format("Vui lòng chọn loại xe trên chuyến xe '{0}'!", cxRef.Name));
                        //string loaixe = "";
                        //switch (((OptionSetValue)cxE["new_loaixe"]).Value)
                        //{
                        //    case 100000000:
                        //        loaixe = "01";//Xe tải
                        //        break;
                        //    case 100000001:
                        //        loaixe = "02";//Xe kéo
                        //        break;
                        //    case 100000002:
                        //        loaixe = "03";//container
                        //        break;
                        //    case 100000003:
                        //        loaixe = "04";//máy kéo
                        //        break;
                        //}

                        //int currentPos = 0;
                        //DateTime currentDate = DateTime.Now.Date;
                        //StringBuilder fetchXml = new StringBuilder();
                        //fetchXml.AppendLine("<fetch version='1.0' output-format='xml-platform' mapping='logical' aggregate='true' distinct='true' >");
                        //fetchXml.AppendLine("<entity name='new_phieudangtai'>");
                        //fetchXml.AppendLine("<attribute name='new_phieudangtaiid' alias='count' aggregate='count'/>");
                        //fetchXml.AppendLine("<filter type='and'>");
                        //fetchXml.AppendLine(string.Format("<condition attribute='new_lenhdon' operator='eq' value='{0}'/>", ldRef.Id));
                        //fetchXml.AppendLine(string.Format("<condition attribute='new_ngay' operator='eq' value='{0:yyyy-MM-dd}'/>", DateTime.Now));
                        //fetchXml.AppendLine("</filter>");
                        //fetchXml.AppendLine("</entity>");
                        //fetchXml.AppendLine("</fetch>");
                        //EntityCollection etc = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                        //if (etc.Entities.Count() > 0)
                        //{
                        //    Entity c = etc.Entities[0];
                        //    currentPos = (int)((AliasedValue)c["count"]).Value;
                        //}
                        //string prefix = string.Format("{0:yy}{1:MMdd}{2}", bDate, DateTime.Now, loaixe);
                        //string sufix = "";
                        ////ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        //string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        //if (!string.IsNullOrWhiteSpace(field))
                        //{
                        //    currentPos++;
                        //    var crLength = length - currentPos.ToString().Length;
                        //    if (crLength < 0)
                        //    {
                        //        currentPos = 1;
                        //        crLength = length - 1;
                        //    }
                        //    string middle = "";
                        //    for (int i = 0; i < crLength; i++)
                        //        middle += "0";

                        //    eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                        //}
                    }
                    break;
                #endregion
                #region new_phieudangkydichvu
                case "new_phieudangkydichvu":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = string.Format("DKDV{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieudangkyhomgiong
                case "new_phieudangkyhomgiong":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = string.Format("DKHG{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieudangkythuoc
                case "new_phieudangkythuoc":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("DKTHUOC{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieudangkyphanbon
                case "new_phieudangkyphanbon":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("DKPB{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            service.Update(Autonumberdetail);

                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieudangkyvattu
                case "new_phieudangkyvattu":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("DKVT{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieucan
                case "new_phieucan":
                    {

                        if (!eField.Attributes.Contains("new_lenhdon"))
                            throw new Exception("Vui lòng chọn lệnh đốn!");

                        int loaican = eField.Contains("new_loaican")
                            ? ((OptionSetValue)eField["new_loaican"]).Value
                            : 0;

                        if (loaican == 100000001 || loaican == 0)
                            return;

                        EntityReference ldRef = (EntityReference)eField["new_lenhdon"];
                        Entity ldE = service.Retrieve(ldRef.LogicalName, ldRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (ldE == null)
                            throw new Exception(string.Format("Lệnh đốn '{0}' không tồn tại hoặc bị xóa!", ldRef.Name));
                        if (!ldE.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên lệnh đốn '{0}'!", ldRef.Name));

                        EntityReference dtRef = (EntityReference)ldE["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu tại vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu tại vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("PC{0:yyyy}", bDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieuthamdinhdautu
                case "new_phieuthamdinhdautu":
                    {

                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("TDDT{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieudotapchat
                case "new_phieudotapchat":
                    {
                        if (!eField.Contains("new_lenhdon"))
                            throw new Exception("Vui lòng chọn lệnh đốn!");
                        EntityReference ldRef = (EntityReference)eField["new_lenhdon"];
                        Entity ld = service.Retrieve(ldRef.LogicalName, ldRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (ld == null)
                            throw new Exception(string.Format("Lệnh đốn {0} không tồn tại hoăc bị xóa!", ldRef.Name));


                        if (!ld.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên lệnh đốn {0}!", ldRef.Name));
                        EntityReference dtRef = (EntityReference)ld["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("L{0:yy}-{1:yy}", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieudoccs
                case "new_phieudoccs":
                    {
                        if (!eField.Contains("new_lenhdon"))
                            throw new Exception("Vui lòng chọn lệnh đốn!");
                        EntityReference ldRef = (EntityReference)eField["new_lenhdon"];
                        Entity ld = service.Retrieve(ldRef.LogicalName, ldRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (ld == null)
                            throw new Exception(string.Format("Lệnh đốn {0} không tồn tại hoăc bị xóa!", ldRef.Name));


                        if (!ld.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên lệnh đốn {0}!", ldRef.Name));
                        EntityReference dtRef = (EntityReference)ld["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("L{0:yy}-{1:yy}", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieugiaonhanvattu
                case "new_phieugiaonhanvattu":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("GNVT{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieugiaonhanthuoc
                case "new_phieugiaonhanthuoc":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("T{0:yy}-{1:yy}", bDate, eDate);
                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieutamung
                case "new_phieutamung":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("DNTU{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieuchitienmat
                case "new_phieuchitienmat":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("LC{0:yy}-{1:yy}", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieuthu
                case "new_phieuthu":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("PM{0:yy}-{1:yy}", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_danhgianangsuat
                case "new_danhgianangsuat":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("NTBS{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthuboclamia
                case "new_nghiemthuboclamia":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));

                        //get key
                        string key = (string)dt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("NTBL{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthuchatsatgoc
                case "new_nghiemthuchatsatgoc":
                    {
                        if (!eField.Attributes.Contains("new_hopdongdautumia"))
                            throw new Exception("Vui lòng chọn hợp đồng tư mía!");
                        EntityReference hdref = (EntityReference)eField["new_hopdongdautumia"];
                        Entity hd = service.Retrieve(hdref.LogicalName, hdref.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!", hdref.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư mía '{0}'!", hdref.Name));
                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng đầu tư mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdref.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];


                        string prefix = string.Format("NTSTH{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_nghiemthucongtrinh
                case "new_nghiemthucongtrinh":
                    {
                        if (!eField.Attributes.Contains("new_hopdongdautuhatang"))
                            throw new Exception("Vui lòng chọn hợp đồng đầu tư hạ tầng!");
                        EntityReference hdref = (EntityReference)eField["new_hopdongdautuhatang"];
                        Entity hd = service.Retrieve(hdref.LogicalName, hdref.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng đầu tư hạ tầng'{0}' không tồn tại hoặc bị xóa!", hdref.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư hạ tầng '{0}'!", hdref.Name));
                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng đầu tư mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdref.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];


                        string prefix = string.Format("NTCT{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieudenghithanhtoan
                case "new_phieudenghithanhtoan":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        int typeNum = ((OptionSetValue)eField["new_loaithanhtoan"]).Value;
                        string type = string.Empty;
                        switch (typeNum)
                        {
                            case 100000000://cung cap dich vu
                                type = "DV";
                                break;
                            case 100000001://hop dong dau tu ha tang
                                type = "HT";
                                break;
                            //case 100000002://Phân bón
                            //    type = "PB";
                            //    break;
                            case 100000003://hom giong mua cua nong danf
                                type = "HG";
                                break;
                        }

                        string prefix = string.Format("DNTT{0}{1:yy}{2:yy}-", type, begin, end);
                        string sufix = "";
                        
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                service.Update(eAu);
                                CreateAutoNumberdetail(currentPos,eAu,key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieudenghithuong
                case "new_phieudenghithuong":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("DNTHUONG{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieudenghithuno
                case "new_phieudenghithuno":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("DNTHUNO{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_yeucaugiaichap
                case "new_yeucaugiaichap":
                    {
                        string prefix = "YCGC" + DateTime.Now.Year.ToString() + "-";
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieutinhtienmia
                case "new_phieutinhtienmia":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];
                        int yDate = begin.Year + 1;

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        string prefix = "PTTM" + yDate + "-";
                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region new_phieutinhtienkhuyenkhich
                case "new_phieutinhtienkhuyenkhich":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("PTTKK{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_bangketienmia
                case "new_bangketienmia":
                    {
                        //if (!eField.Attributes.Contains("new_loaihopdong"))
                        //    throw new Exception("Vui lòng chọn loại hộp đồng!");
                        //int type = ((OptionSetValue)eField["new_loaihopdong"]).Value;
                        //Entity hd = null;
                        //EntityReference vdtRef = null;
                        //if (type == 100000000)//HĐ đầu tư mía
                        //{
                        //    if (!eField.Attributes.Contains("new_hopdongdautumia"))
                        //        throw new Exception("Vui lòng chọn hợp đồng đầu tư mía!");
                        //    EntityReference hdRef = eField["new_hopdongdautumia"] as EntityReference;
                        //    hd = service.Retrieve(hdRef.LogicalName, hdRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        //    if (hd == null)
                        //        throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!", hdRef.Name));
                        //    if (!hd.Contains("new_vudautu"))
                        //        throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư mía '{0}'", hdRef.Name));
                        //}
                        //else if (type == 100000001)//HĐ thu hoạch
                        //{
                        //    if (!eField.Attributes.Contains("new_hopdongthuhoach"))
                        //        throw new Exception("Vui lòng chọn hợp đồng thu hoạch!");
                        //    EntityReference hdRef = eField["new_hopdongthuhoach"] as EntityReference;
                        //    hd = service.Retrieve(hdRef.LogicalName, hdRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        //    if (hd == null)
                        //        throw new Exception(string.Format("Hợp đồng thu hoạch '{0}' không tồn tại hoặc bị xóa!", hdRef.Name));
                        //    if (!hd.Contains("new_vudautu"))
                        //        throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng thu hoạch '{0}'", hdRef.Name));
                        //}
                        //else if (type == 100000002)//HĐ vận chuyển
                        //{
                        //    if (!eField.Attributes.Contains("new_hopdongvanchuyen"))
                        //        throw new Exception("Vui lòng chọn hợp đồng đầu vận chuyển!");
                        //    EntityReference hdRef = eField["new_hopdongvanchuyen"] as EntityReference;
                        //    hd = service.Retrieve(hdRef.LogicalName, hdRef.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        //    if (hd == null)
                        //        throw new Exception(string.Format("Hợp đồng đầu vận chuyển '{0}' không tồn tại hoặc bị xóa!", hdRef.Name));
                        //    if (!hd.Contains("new_vudautu"))
                        //        throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu vận chuyển '{0}'", hdRef.Name));

                        //}
                        //vdtRef = (EntityReference)hd["new_vudautu"];
                        //Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        //if (vdt == null)
                        //    throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        //if (!vdt.Attributes.Contains("new_ngaybatdau"))
                        //    throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        //if (!vdt.Attributes.Contains("new_ngayketthuc"))
                        //    throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        //DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        //DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        //if (!eField.Attributes.Contains("new_tungay"))
                        //    throw new Exception("Vui lòng chọn từ ngày!");
                        //if (!eField.Attributes.Contains("new_denngay"))
                        //    throw new Exception("Vui lòng chọn đến ngày!");
                        //DateTime begin = (DateTime)eField["new_tungay"];
                        //DateTime end = (DateTime)eField["new_denngay"];

                        //string prefix = string.Format("{0:dd/MM/yyyy}-{1:dd/MM/yyyy}", begin, end);
                        //string sufix = "";
                        //ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        //string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        //if (!string.IsNullOrWhiteSpace(field))
                        //{
                        //    currentPos++;
                        //    var crLength = length - currentPos.ToString().Length;
                        //    if (crLength < 0)
                        //    {
                        //        currentPos = 1;
                        //        crLength = length - 1;
                        //    }
                        //    string middle = "";
                        //    for (int i = 0; i < crLength; i++)
                        //        middle += "0";

                        //    eField[field] = string.Format("{0}-{1}{2}{3}", prefix, middle, currentPos, sufix);
                        //    eAu["bsd_currentposition"] = currentPos.ToString();
                        //    flag = true;
                        //    service.Update(eAu);
                        //}
                        //if (!eField.Contains("new_vudautu"))
                        //    throw new Exception("Vui lòng chọn vụ đầu tư!");
                        //EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        //Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        //if (dt == null)
                        //    throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        //if (!dt.Attributes.Contains("new_ngaybatdau"))
                        //    throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        //if (!dt.Attributes.Contains("new_ngayketthuc"))
                        //    throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        //DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        //DateTime eDate = (DateTime)dt["new_ngayketthuc"];
                        int year = DateTime.Now.Year;

                        string prefix = string.Format("LC{0}-", year);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_banggiadichvu
                case "new_banggiadichvu":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = "/DV/" + String.Format("{0:yy}-{1:yy}", bDate, eDate);
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phanbodautu
                case "new_phanbodautu":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "PB" + String.Format("{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_banggiagiong
                case "new_banggiagiong":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = "/GM/" + String.Format("{0:yy}-{1:yy}", bDate, eDate);
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_banggiaphanbon
                case "new_banggiaphanbon":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = "/PB/" + String.Format("{0:yy}-{1:yy}", bDate, eDate);
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_banggiathuoc
                case "new_banggiathuoc":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = "/T/" + String.Format("{0:yy}-{1:yy}", bDate, eDate);
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_banggiavattukhac
                case "new_banggiavattukhac":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception("Vụ đầu tư '" + dtRef.Name + "' không tồn tại hoặc bị xóa!");

                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception("Vui lòng chọn ngày bắt đầu tại vụ đầu tư:'" + dtRef.Name + "'!");
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception("Vui lòng chọ ngày kết thúc tại vụ đầu tư:'" + dtRef.Name + "'!");

                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = "";
                        string sufix = "/VTK/" + String.Format("{0:yy}-{1:yy}", bDate, eDate);
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieuchuyenno
                case "new_phieuchuyenno":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("PCNO{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_phieudieuchinhcongno
                case "new_phieudieuchinhcongno":
                    {
                        if (!eField.Contains("new_vudautudieuchinh"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautudieuchinh"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("DCNO{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_bienbangiamhuydientich
                case "new_bienbangiamhuydientich":
                    {
                        if (!eField.Attributes.Contains("new_hopdongdautumia"))
                            throw new Exception("Vui lòng chọn hợp đồng tư mía!");
                        EntityReference hdref = (EntityReference)eField["new_hopdongdautumia"];
                        Entity hd = service.Retrieve(hdref.LogicalName, hdref.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!", hdref.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư mía '{0}'!", hdref.Name));
                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng đầu tư mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdref.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];


                        string prefix = string.Format("BBGHDT{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_bienbanmiachay
                case "new_bienbanmiachay":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("BBMC{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_bienbanthoathuancongdon
                case "new_bienbanthoathuancongdon":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("BBTTCD{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_bienbanthuhoachsom
                case "new_bienbanthuhoachsom":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("BBTHS{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_bienbanvipham
                case "new_bienbanvipham":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("BBVP{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_taisanthechap
                case "new_taisanthechap":
                    {
                        string prefix = "TSTC";
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            tracingService.Trace("a");
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";
                            tracingService.Trace("b");
                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_nhatkydongruong
                case "new_nhatkydongruong":
                    {
                        if (!eField.Attributes.Contains("new_hopdongdautumia"))
                            throw new Exception("Vui lòng chọn hợp đồng tư mía!");
                        EntityReference hdref = (EntityReference)eField["new_hopdongdautumia"];
                        Entity hd = service.Retrieve(hdref.LogicalName, hdref.Id, new ColumnSet(new string[] { "new_vudautu" }));
                        if (hd == null)
                            throw new Exception(string.Format("Hợp đồng đầu tư mía '{0}' không tồn tại hoặc bị xóa!", hdref.Name));
                        if (!hd.Attributes.Contains("new_vudautu"))
                            throw new Exception(string.Format("Vui lòng chọn vụ đầu tư trên hợp đồng đầu tư mía '{0}'!", hdref.Name));
                        EntityReference vdtRef = (EntityReference)hd["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' trên hợp đồng đầu tư mía '{1}' không tồn tại hoặc đã bị xóa!", vdtRef.Name, hdref.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));
                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("NK{0:yy}{1:yy}-", begin, end);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region opportunity
                case "opportunity":
                    {
                        if (!eField.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference dtRef = (EntityReference)eField["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));

                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("DNDT{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_bienbancando
                case "new_bienbancando":
                    {
                        if (!eField.Contains("new_lenhdon"))
                            throw new Exception("Vui lòng chọn lệnh đốn!");

                        Entity lenhdon = service.Retrieve("new_lenhdon", ((EntityReference)eField["new_lenhdon"]).Id,
                            new ColumnSet(new string[] { "new_vudautu" }));

                        EntityReference dtRef = (EntityReference)lenhdon["new_vudautu"];
                        Entity dt = service.Retrieve(dtRef.LogicalName, dtRef.Id, new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc" }));
                        if (dt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc bị xóa!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        if (!dt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc đầu trên vụ đầu tư '{0}'!", dtRef.Name));
                        DateTime bDate = (DateTime)dt["new_ngaybatdau"];
                        DateTime eDate = (DateTime)dt["new_ngayketthuc"];

                        string prefix = string.Format("BBCD{0:yy}{1:yy}-", bDate, eDate);
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                #endregion
                #region new_hopdongthuedat
                case "new_hopdongvanchuyen_xevanchuyen":
                    {
                        if (!eField.Contains("new_hopdongvanchuyen"))
                            throw new Exception("Thẻ xe không có hợp đồng vận chuyển");

                        Entity hopdongvanchuyen = service.Retrieve("new_hopdongvanchuyen", ((EntityReference)eField["new_hopdongvanchuyen"]).Id,
                            new ColumnSet(new string[] { "new_vudautu" }));

                        if (!hopdongvanchuyen.Contains("new_vudautu"))
                            throw new Exception("Hợp đồng vận chuyển không có vụ đầu tư");

                        Entity vdt = service.Retrieve("new_vudautu", ((EntityReference)hopdongvanchuyen["new_vudautu"]).Id,
                            new ColumnSet(new string[] { "new_ngayketthuc", "new_mavudautu" }));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("T{0:yy}", begin);
                        string sufix = "";
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                        string middle = "";

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                CreateAutoNumberdetail(currentPos, eAu, key);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }

                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                                UpdateAutoNumberdetail(Autonumberdetail, currentPos);
                            }
                        }
                    }
                    break;
                #endregion
                #region etltransaction
                case "new_etltransaction":
                    {

                        if (!eField.Attributes.Contains("new_invoicetype"))
                            throw new Exception("Vui lòng chọn invoicetype!");

                        string invoicetype = (string)eField["new_invoicetype"];
                        string prefix = invoicetype;

                        if (((OptionSetValue)eAu["bsd_grouptype"]).Value == 100000001) // with detail
                        {
                            StringBuilder str = new StringBuilder();
                            Entity Autonumberdetail = RetrieveBsdAutonumberdetail(prefix, eAu);

                            if (Autonumberdetail == null)
                            {
                                Entity en = new Entity("bsd_autonumberdetail");

                                string sufix = "";
                                ulong currentPos = 0;
                                string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                                if (!string.IsNullOrWhiteSpace(field))
                                {
                                    currentPos++;
                                    var crLength = length - currentPos.ToString().Length;
                                    if (crLength < 0)
                                    {
                                        currentPos = 1;
                                        crLength = length - 1;
                                    }
                                    string middle = "-";
                                    for (int i = 0; i < crLength; i++)
                                        middle += "0";

                                    eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                    eAu["bsd_currentposition"] = currentPos.ToString();
                                    flag = true;

                                    en["bsd_currentposition"] = currentPos + "";
                                    en["bsd_name"] = prefix;
                                    en["bsd_autonumberid"] = new EntityReference(eAu.LogicalName, eAu.Id);
                                    service.Create(en);
                                    service.Update(eAu);
                                }
                            }
                            else
                            {
                                string sufix = "";
                                ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                                string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;
                                if (!string.IsNullOrWhiteSpace(field))
                                {
                                    currentPos++;
                                    var crLength = length - currentPos.ToString().Length;
                                    if (crLength < 0)
                                    {
                                        currentPos = 1;
                                        crLength = length - 1;
                                    }
                                    string middle = "-";
                                    for (int i = 0; i < crLength; i++)
                                        middle += "0";

                                    eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                    eAu["bsd_currentposition"] = currentPos.ToString();
                                    flag = true;

                                    Autonumberdetail["bsd_currentposition"] = currentPos + "";

                                    service.Update(Autonumberdetail);
                                    service.Update(eAu);
                                }

                            }
                        }

                    }
                    break;
                #endregion
                #region new_phieudenghitamung
                case "new_phieudenghitamung":
                    {
                        if (!eField.Attributes.Contains("new_vudautu"))
                            throw new Exception("Vui lòng chọn vụ đầu tư!");
                        EntityReference vdtRef = (EntityReference)eField["new_vudautu"];
                        Entity vdt = service.Retrieve(vdtRef.LogicalName, vdtRef.Id,
                            new ColumnSet(new string[] { "new_ngaybatdau", "new_ngayketthuc", "new_mavudautu" }));

                        if (vdt == null)
                            throw new Exception(string.Format("Vụ đầu tư '{0}' không tồn tại hoặc đã bị xóa!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngaybatdau"))
                            throw new Exception(string.Format("Vui lòng chọn ngày bắt đầu trên vụ đầu tư '{0}'!", vdtRef.Name));
                        if (!vdt.Attributes.Contains("new_ngayketthuc"))
                            throw new Exception(string.Format("Vui lòng chọn ngày kết thúc trên vụ đầu tư '{0}'!", vdtRef.Name));

                        //get key
                        string key = (string)vdt["new_mavudautu"];
                        Entity Autonumberdetail = RetrieveBsdAutonumberdetail(key, eAu);

                        DateTime begin = (DateTime)vdt["new_ngaybatdau"];
                        DateTime end = (DateTime)vdt["new_ngayketthuc"];

                        string prefix = string.Format("DNTU{0:yy}{1:yy}-", begin, end);
                        string sufix = "";

                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (Autonumberdetail == null)
                        {
                            ulong currentPos = 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }
                                string middle = "";
                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                            }
                        }
                        else
                        {
                            ulong currentPos = Autonumberdetail.Attributes.Contains("bsd_currentposition") ? ulong.Parse(Autonumberdetail["bsd_currentposition"].ToString()) : 0;

                            if (!string.IsNullOrWhiteSpace(field))
                            {
                                currentPos++;
                                var crLength = length - currentPos.ToString().Length;
                                if (crLength < 0)
                                {
                                    currentPos = 1;
                                    crLength = length - 1;
                                }
                                string middle = "";
                                for (int i = 0; i < crLength; i++)
                                    middle += "0";

                                eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                                //eAu["bsd_currentposition"] = currentPos.ToString();
                                flag = true;
                                //service.Update(eAu);
                            }
                        }
                    }
                    break;

                #endregion
                #region
                case "new_taikhoannganhang":
                    {
                        string prefix = DateTime.Now.Year + "-"; ;
                        string sufix = "";
                        ulong currentPos = eAu.Attributes.Contains("bsd_currentposition") ? ulong.Parse(eAu["bsd_currentposition"].ToString()) : 0;
                        string field = eAu.Attributes.Contains("bsd_fieldlogical") ? eAu["bsd_fieldlogical"].ToString() : string.Empty;

                        if (!string.IsNullOrWhiteSpace(field))
                        {
                            currentPos++;
                            var crLength = length - currentPos.ToString().Length;
                            if (crLength < 0)
                            {
                                currentPos = 1;
                                crLength = length - 1;
                            }
                            string middle = "";
                            for (int i = 0; i < crLength; i++)
                                middle += "0";

                            eField[field] = string.Format("{0}{1}{2}{3}", prefix, middle, currentPos, sufix);
                            eAu["bsd_currentposition"] = currentPos.ToString();
                            flag = true;
                            service.Update(eAu);
                        }
                    }
                    break;
                    #endregion
            }
        }

        Entity RetrieveBsdAutonumberdetail(String GroupValue, Entity eAu)
        {
            Entity kq = null;
            QueryExpression q = new QueryExpression("bsd_autonumberdetail");
            q.ColumnSet = new ColumnSet(new string[] { "bsd_currentposition", "bsd_name" });
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("bsd_autonumberid", ConditionOperator.Equal, eAu.Id));
            q.Criteria.AddCondition(new ConditionExpression("bsd_name", ConditionOperator.Equal, GroupValue));
            EntityCollection entc = service.RetrieveMultiple(q);
            //bsd_currentposition
            if (entc.Entities.Count >= 1)
            {
                kq = entc.Entities[0];
            }
            else
                kq = null;
            return kq;
        }

        private EntityCollection RetrieveAutoNumbers(string entityName)
        {
            StringBuilder xml = new StringBuilder();
            xml.AppendLine("<?xml version='1.0' encoding='utf-8'?>");
            xml.AppendLine("<fetch mapping='logical'>");
            xml.AppendLine("<entity name='bsd_autonumber'>");
            xml.AppendLine("<attribute name='bsd_autonumberid' />");
            xml.AppendLine("<attribute name='bsd_name'/>");
            xml.AppendLine("<attribute name='bsd_entitylogical'/>");
            xml.AppendLine("<attribute name='bsd_field' />");
            xml.AppendLine("<attribute name='bsd_fieldlogical'/>");
            xml.AppendLine("<attribute name='bsd_prefix' />");
            xml.AppendLine("<attribute name='bsd_sufix' />");
            xml.AppendLine("<attribute name='bsd_currentposition' />");
            xml.AppendLine("<attribute name='bsd_length' />");
            xml.AppendLine("<attribute name='bsd_usecustom' />");
            xml.AppendLine("<attribute name='bsd_grouptype' />");
            xml.AppendLine("<attribute name='bsd_groupfield' />");
            xml.AppendLine("<filter>");
            xml.AppendLine("<condition attribute='bsd_entitylogical' operator='eq' value='" + entityName + "' />");
            xml.AppendLine("<condition attribute='statecode' operator='eq' value='0' />");
            xml.AppendLine("</filter>");
            xml.AppendLine("</entity>");
            xml.AppendLine("</fetch>");
            return service.RetrieveMultiple(new FetchExpression(xml.ToString()));
        }

        private int Count(string entityName)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("<?xml version='1.0' encoding='utf-8'?>");
            sb.AppendLine("<fetch aggregate='true' mapping='logical'>");
            sb.AppendLine("<entity name='bsd_autonumber'>");
            sb.AppendLine("<attribute name='bsd_autonumberid' alias='rs' aggregate='count' />");
            sb.AppendLine("<filter>");
            sb.AppendLine("<condition attribute='bsd_entitylogical' operator='eq' value='" + entityName + "'/>");
            sb.AppendLine("<condition attribute='statecode' operator='eq' value='0'/>");
            sb.AppendLine("</filter>");
            sb.AppendLine("</entity>");
            sb.AppendLine("</fetch>");
            EntityCollection enc = service.RetrieveMultiple(new FetchExpression(sb.ToString()));
            if (enc.Entities.Count > 0)
            {
                Entity en = enc.Entities[0];
                if (en.Attributes.Contains("rs"))
                    return (int)((AliasedValue)enc.Entities[0]["rs"]).Value;
                else
                    return 0;

            }
            else
                return 0;
        }

        private bool IsExisted(string entityName, string fieldName)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("<?xml version='1.0' encoding='utf-8'?>");
            sb.AppendLine("<fetch aggregate='true' mapping='logical'>");
            sb.AppendLine("<entity name='bsd_autonumber'>");
            sb.AppendLine("<attribute name='bsd_autonumberid' alias='rs' aggregate='count' />");
            sb.AppendLine("<filter type='and'>");
            sb.AppendLine("<condition attribute='bsd_entitylogical' operator='eq' value='" + entityName + "'/>");
            sb.AppendLine("<condition attribute='bsd_fieldlogical' operator='eq' value='" + fieldName + "'/>");
            sb.AppendLine("</filter>");
            sb.AppendLine("</entity>");
            sb.AppendLine("</fetch>");
            EntityCollection enc = service.RetrieveMultiple(new FetchExpression(sb.ToString()));
            if (enc.Entities.Count > 0)
            {
                Entity en = enc.Entities[0];
                if (en.Attributes.Contains("rs"))
                    return ((int)((AliasedValue)enc.Entities[0]["rs"]).Value > 0);
                else
                    return true;
            }
            else
                return true;
        }

        private Entity Vuhientai()
        {
            StringBuilder fetchXml = new StringBuilder();
            fetchXml.Append("<fetch mapping='logical' version='1.0'>");
            fetchXml.Append("<entity name='new_vudautu'>");
            fetchXml.Append("<attribute name='new_nambatdau'/>");
            fetchXml.Append("<attribute name='new_ngaybatdau'/>");
            fetchXml.Append("<attribute name='new_ngayketthuc'/>");
            fetchXml.Append("<attribute name='new_mavudautu'/>");
            fetchXml.Append("<filter type='and'>");
            fetchXml.Append(string.Format("<condition attribute='new_danghoatdong' operator='eq' value='{0}'></condition>", true));
            fetchXml.Append("</filter>");
            fetchXml.Append("</entity>");
            fetchXml.Append("</fetch>");

            EntityCollection entc = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
            return entc.Entities[0];
        }

        void CreateAutoNumberdetail(ulong currentPos, Entity eAu, string key)
        {
            Entity en = new Entity("bsd_autonumberdetail");

            en["bsd_currentposition"] = currentPos + "";
            en["bsd_name"] = key;
            en["bsd_autonumberid"] = new EntityReference(eAu.LogicalName, eAu.Id);
            service.Create(en);
        }

        void UpdateAutoNumberdetail(Entity Autonumberdetail, ulong currentPos)
        {
            Autonumberdetail["bsd_currentposition"] = currentPos + "";
            service.Update(Autonumberdetail);
        }
    }
}
