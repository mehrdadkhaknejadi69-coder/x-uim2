# 🚀 VodiWalker 15.0.0

VodiWalker یک کنترل‌سنتر حرفه‌ای برای مدیریت لینک‌ها، سابسکریپشن و فروش خودکار است.

## ✨ هسته سرویس
- VLESS روی WebSocket و XHTTP
- مدیریت لینک با حجم، سرعت، IP و انقضا
- سابسکریپشن تکی و گروهی
- QR و صفحات عمومی
- آمار، لاگ، اتصالات زنده و ذخیره‌سازی روی دیسک
- محدودیت سرعت با Token Bucket و جریان XHTTP تطبیقی
- رابط کاملاً بازطراحی‌شده با برند VodiWalker

## 🛒 فروشگاه و ربات فروش
- `/plans` صفحه فروش حرفه‌ای
- ربات فروش عمومی + پنل ادمین در یک Bot
- پلن‌های Starter / Pro / Ultra
- فاکتور و پرداخت Telegram Stars (XTR) یا Provider Token
- تحویل خودکار لینک اشتراک بعد از پرداخت
- ثبت سفارش و مشتری در `vodiwalker_sales.json`
- گزارش فروش در پنل ربات

## 🔐 متغیرهای مهم
`ADMIN_PASSWORD`، `SECRET_KEY`، `TELEGRAM_BOT_TOKEN`، `TELEGRAM_ADMIN_IDS` و در صورت استفاده از پرداخت Provider، `TELEGRAM_PAYMENT_PROVIDER_TOKEN`.

برای Telegram Stars، `TELEGRAM_PAYMENT_CURRENCY=XTR` قرار دهید. قیمت‌ها در `sales.py` و `main.py` قابل تنظیم‌اند.

## 📦 اجرا
```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
```
