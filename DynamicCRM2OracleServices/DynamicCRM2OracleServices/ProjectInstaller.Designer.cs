namespace DynamicCRM2OracleServices
{
    partial class ProjectInstaller
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.DynamicCRM2OracleServiceInstaller = new System.ServiceProcess.ServiceProcessInstaller();
            this.DynamicCRM2Oracle = new System.ServiceProcess.ServiceInstaller();
            // 
            // DynamicCRM2OracleServiceInstaller
            // 
            this.DynamicCRM2OracleServiceInstaller.Account = System.ServiceProcess.ServiceAccount.NetworkService;
            this.DynamicCRM2OracleServiceInstaller.Password = null;
            this.DynamicCRM2OracleServiceInstaller.Username = null;
            // 
            // DynamicCRM2Oracle
            // 
            this.DynamicCRM2Oracle.ServiceName = "DynamicCRM2Oracle";
            this.DynamicCRM2Oracle.StartType = System.ServiceProcess.ServiceStartMode.Automatic;
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.DynamicCRM2OracleServiceInstaller,
            this.DynamicCRM2Oracle});

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller DynamicCRM2OracleServiceInstaller;
        private System.ServiceProcess.ServiceInstaller DynamicCRM2Oracle;
    }
}