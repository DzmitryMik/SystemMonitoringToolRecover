using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SystemMonitoring.AlertRecoveryTool
{
    internal enum AlertExpirationPolicy
    {
        None = 0,
        DoNotPushExpired = 1,
        PushExpiredWithExpirationOverride = 2,
        PushAll = 3
    }
}
