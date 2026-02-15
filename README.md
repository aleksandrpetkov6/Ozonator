# Ozon Seller OS — MVP‑0 (v0.1)

Это исходники MVP‑0: Desktop (macOS/Windows), локальное хранение ключей, синхронизация списка товаров из Ozon Seller API и отображение `offer_id`.

## Требования
- Node.js 20+ (или 18+)
- npm / pnpm

## Запуск (dev)
```bash
npm install
npm run dev
```

## Сборка инсталлятора
```bash
npm run dist
```

> На macOS можно собрать .dmg/.app, на Windows — .exe (NSIS). Для кросс‑сборки используйте CI (GitHub Actions).

## Где лежат данные
- Ключи: локально, в зашифрованном виде через Electron `safeStorage` (файл secrets.json в userData).
- База SQLite: файл app.db в userData.

userData по умолчанию:
- Windows: %APPDATA%\Ozon Seller OS (MVP0)
- macOS: ~/Library/Application Support/Ozon Seller OS (MVP0)
