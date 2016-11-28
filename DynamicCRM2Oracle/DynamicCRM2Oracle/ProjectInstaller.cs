using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicCRM2Oracle
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();
        }

        public override void Install(IDictionary stateSaver)
        {
            serviceInstaller.ServiceName = "DynamicCRM2Oracle";
            const string ServiceNameParameterName = "Dynamic CRM 2 Oracle";
            if (!String.IsNullOrWhiteSpace(Context.Parameters[ServiceNameParameterName]))
            {
                serviceInstaller.ServiceName = Context.Parameters[ServiceNameParameterName];
            }

            serviceProcessInstaller.Account = System.ServiceProcess.ServiceAccount.LocalService;

            base.Install(stateSaver);
        }

        private void serviceProcessInstaller_AfterInstall(object sender, InstallEventArgs e)
        {

        }
    }
}
