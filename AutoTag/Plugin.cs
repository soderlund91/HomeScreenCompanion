using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Plugins;
using MediaBrowser.Model.Plugins;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Drawing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace HomeScreenCompanion
{
    public class Plugin : BasePlugin<PluginConfiguration>, IHasWebPages, IHasThumbImage
    {
        public override string Name => "Home Screen Companion";

        public override Guid Id => new Guid("7c10708f-43e4-4d69-923c-77d01802315b");

        public override string Description => "Auto-tagging, collection management and home screen sync for Emby.";

        public Plugin(IApplicationPaths applicationPaths, IXmlSerializer xmlSerializer)
            : base(applicationPaths, xmlSerializer)
        {
            Instance = this;
            AppPaths = applicationPaths;
            XmlSerializer = xmlSerializer;
        }

        public static Plugin? Instance { get; private set; }
        public static IApplicationPaths AppPaths { get; private set; } = null!;
        public static IXmlSerializer XmlSerializer { get; private set; } = null!;

        public IEnumerable<PluginPageInfo> GetPages()
        {
            var assembly = GetType().Assembly;

            var htmlPath = assembly.GetManifestResourceNames().FirstOrDefault(r => r.EndsWith("configPage.html"));
            var jsPath = assembly.GetManifestResourceNames().FirstOrDefault(r => r.EndsWith("configPage.js"));

            if (htmlPath == null || jsPath == null)
            {
                return new List<PluginPageInfo>();
            }

            return new[]
            {
                new PluginPageInfo
                {
                    Name = "HomeScreenCompanion",
                    EmbeddedResourcePath = htmlPath,

                    EnableInMainMenu = true,
                    DisplayName = "Home Screen Companion",
                    MenuIcon = "home"
                },
                new PluginPageInfo
                {
                    Name = "HomeScreenCompanionJS",
                    EmbeddedResourcePath = jsPath
                }
            };
        }

        public Stream GetThumbImage()
        {
            var type = GetType();
            var thumbPath = type.Assembly.GetManifestResourceNames().FirstOrDefault(r => r.EndsWith("thumb.png"));
            return (thumbPath != null ? type.Assembly.GetManifestResourceStream(thumbPath) : Stream.Null) ?? Stream.Null;
        }

        public ImageFormat ThumbImageFormat => ImageFormat.Png;
    }
}