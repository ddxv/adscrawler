INSERT INTO trackers (id, name) VALUES
(1, 'Google Firebase'),
(2, 'Facebook Analytics'),
(3, 'AppsFlyer'),
(4, 'Adjust'),
(5, 'Kochava'),
(6, 'Google Analytics'),
(7, 'IAB Open Measurement'),
(8, 'Tenjin'),
(9, 'AirBridge'),
(10, 'No Trackers Found'),
(11, 'WanMei 完美');

INSERT INTO networks (id, name) VALUES
(1, 'Google AdMob'),
(2, 'ironSource'),
(3, 'Unity Ads'),
(4, 'AppLovin'),
(5, 'Facebook Ads'),
(6, 'InMobi'),
(7, 'AdColony'),
(8, 'Vungle'),
(9, 'Amazon Advertisement'),
(10, 'One Signal'),
(11, 'Flurry'),
(12, 'ByteDance'),
(13, 'Mintegral'),
(14, 'ChartBoost'),
(15, 'TapJoy'),
(16, 'Fyber'),
(17, 'No Ad Network Detected'),
(18, 'VK'),
(19, 'AdJoe');


INSERT INTO public.tracker_package_map (tracker, package_pattern) VALUES
(1, 'com.google.firebase.analytics'),
(1, 'com.google.android.gms.measurement'),
(1, 'com.google.firebase.firebase_analytics'),
(2, 'com.appsflyer'),
(3, 'com.kochava'),
(4, 'com.adjust.sdk'),
(4, 'com.adjust.android.sdk'),
(5, 'com.facebook.appevents'),
(5, 'com.facebook.marketing'),
(5, 'com.facebook.CampaignTrackingReceiver');

INSERT INTO public.tracker_package_map (tracker, package_pattern) VALUES
(6, 'com.google.android.apps.analytics'),
(6, 'com.google.android.gms.analytics'),
(6, 'com.google.analytics'),
(7, 'com.iab.omid.library'),
(7, 'com.prime31.util.IabHelperImpl'),
(7, 'com.prime31.IAB'),
(8, 'com.tenjin.android'),
(9, 'io.airbridge'),
(11, 'com.wpsdk'),
(1, 'com.google.firebase');


INSERT INTO public.network_package_map (network, package_pattern) VALUES
(1, 'com.google.ads'),
(1, 'com.google.android.gms.ads'),
(1, 'com.google.android.ads'),
(1, 'com.google.unity.ads'),
(1, 'com.google.android.gms.admob'),
(1, 'com.google.firebase.firebase_ads'),
(2, 'com.ironsource'),
(3, 'com.unity3d.services'),
(3, 'com.unity3d.ads'),
(4, 'com.applovin');

INSERT INTO public.network_package_map (network, package_pattern) VALUES
(5, 'com.facebook.ads'),
(6, 'com.inmobi'),
(6, 'in.inmobi'),
(7, 'com.adcolony'),
(7, 'com.jirbo.adcolony'),
(8, 'com.vungle.publisher'),
(8, 'com.vungle.warren'),
(9, 'com.amazon.device.ads'),
(10, 'com.onesignal'),
(11, 'com.flurry'),
(5, 'com.facebook.sdk.ApplicationId');

INSERT INTO public.network_package_map (network, package_pattern) VALUES
(12, 'com.bytedance'),
(12, 'com.pgl'),
(12, 'com.pangle.global'),
(13, 'com.mintegral'),
(13, 'com.mbridge.msdk'),
(14, 'com.chartboost.sdk'),
(15, 'com.tapjoy'),
(16, 'com.fyber'),
(18, 'com.vk.sdk'),
(18, 'com.vk.api.sdk'),
(19, 'io.adjoe.sdk');
