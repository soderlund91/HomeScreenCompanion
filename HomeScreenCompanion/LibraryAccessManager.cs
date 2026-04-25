using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Querying;
using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace HomeScreenCompanion
{
    public class LibraryAccessManager : IServerEntryPoint
    {
        private readonly ILibraryManager _libraryManager;
        private readonly IUserManager _userManager;
        private readonly ILogger _logger;

        public LibraryAccessManager(
            ILibraryManager libraryManager,
            IUserManager userManager,
            ILogManager logManager)
        {
            _libraryManager = libraryManager;
            _userManager = userManager;
            _logger = logManager.GetLogger("HomeScreenCompanion_LibraryAccess");
        }

        public void Run()
        {
            _libraryManager.ItemAdded += OnItemAdded;
        }

        public void Dispose()
        {
            _libraryManager.ItemAdded -= OnItemAdded;
        }

        private void OnItemAdded(object sender, ItemChangeEventArgs e)
        {
            if (!(e.Item is CollectionFolder)) return;

            // EnabledFolders stores Guid without dashes (lowercase), matching the format
            // Emby uses internally and what existing restricted-user policies contain.
            var libId = e.Item.Id.ToString("N").ToLowerInvariant();
            var libName = e.Item.Name;
            _logger.Info($"[LibraryAccess] New library detected: '{libName}' (Guid={libId}, InternalId={e.Item.InternalId}). Scheduling access grant in 15 s...");

            // Delay to let Emby finish its own async policy modifications after library creation.
            // Emby temporarily sets EnableAllFolders=true for all users during creation, then
            // restores the original values. We run after that window to read accurate policies.
            Task.Delay(15000).ContinueWith(_ => GrantAccessToAllUsers(libId, libName));
        }

        private void GrantAccessToAllUsers(string libId, string libName)
        {
            try
            {
                var bf = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
                var mgrType = _userManager.GetType();

                var getPolicyMethod = mgrType.GetMethods(bf)
                    .Where(m => m.Name == "GetUserPolicy")
                    .OrderBy(m => m.GetParameters().Length)
                    .FirstOrDefault()
                    ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "GetUserPolicy");

                var updateMethod = mgrType.GetMethods(bf)
                    .Where(m => m.Name == "UpdateUserPolicy")
                    .OrderBy(m => m.GetParameters().Length)
                    .FirstOrDefault()
                    ?? typeof(IUserManager).GetMethods().FirstOrDefault(m => m.Name == "UpdateUserPolicy");

                if (updateMethod == null)
                {
                    _logger.Warn("[LibraryAccess] UpdateUserPolicy not found via reflection — skipping.");
                    return;
                }

                var users = _userManager.GetUserList(new UserQuery { IsDisabled = false });
                int updated = 0;

                foreach (var user in users)
                {
                    try
                    {
                        object policy = null;
                        if (getPolicyMethod != null)
                        {
                            try
                            {
                                var gp = getPolicyMethod.GetParameters();
                                policy = getPolicyMethod.Invoke(_userManager,
                                    BuildArgArray(gp, BuildUserIdArg(gp[0].ParameterType, user), null));
                            }
                            catch { }
                        }

                        if (policy == null)
                            policy = user.GetType().GetProperty("Policy")?.GetValue(user);

                        if (policy == null)
                        {
                            _logger.Warn($"[LibraryAccess] No policy for user '{user.Name}' — skipped.");
                            continue;
                        }

                        var enableAllProp = policy.GetType().GetProperty("EnableAllFolders");
                        var enableAll = enableAllProp?.GetValue(policy) is true;
                        _logger.Debug($"[LibraryAccess] User '{user.Name}': EnableAllFolders={enableAll}");

                        if (enableAll) continue;

                        var foldersProp = policy.GetType().GetProperty("EnabledFolders");
                        var folders = foldersProp?.GetValue(policy) as string[] ?? Array.Empty<string>();

                        if (folders.Any(f => string.Equals(f, libId, StringComparison.OrdinalIgnoreCase)))
                        {
                            _logger.Debug($"[LibraryAccess] User '{user.Name}' already has '{libName}' — skipped.");
                            continue;
                        }

                        foldersProp?.SetValue(policy, folders.Concat(new[] { libId }).ToArray());

                        var up = updateMethod.GetParameters();
                        updateMethod.Invoke(_userManager,
                            BuildArgArray(up, BuildUserIdArg(up[0].ParameterType, user), policy));

                        updated++;
                        _logger.Info($"[LibraryAccess] Granted '{libName}' access to user '{user.Name}'.");
                    }
                    catch (Exception ex)
                    {
                        _logger.Error($"[LibraryAccess] Error for user '{user.Name}': {ex.GetBaseException().Message}");
                    }
                }

                _logger.Info($"[LibraryAccess] Done. Granted '{libName}' access to {updated} user(s).");
            }
            catch (Exception ex)
            {
                _logger.Error($"[LibraryAccess] GrantAccessToAllUsers failed: {ex.GetBaseException().Message}");
            }
        }

        private object BuildUserIdArg(Type paramType, BaseItem user)
        {
            if (paramType == typeof(long) || paramType == typeof(Int64))
                return _userManager.GetInternalId(user.Id.ToString());
            if (paramType == typeof(Guid)) return user.Id;
            if (paramType == typeof(string)) return user.Id.ToString();
            return user;
        }

        private static object[] BuildArgArray(ParameterInfo[] parms, object arg0, object arg1)
        {
            var args = new object[parms.Length];
            args[0] = arg0;
            for (int i = 1; i < parms.Length; i++)
            {
                if (i == 1 && arg1 != null) args[i] = arg1;
                else if (parms[i].ParameterType == typeof(CancellationToken)) args[i] = CancellationToken.None;
                else if (parms[i].HasDefaultValue) args[i] = parms[i].DefaultValue;
                else args[i] = null;
            }
            return args;
        }
    }
}
