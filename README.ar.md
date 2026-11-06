<p align="center"><img src="apps/desktop/src-tauri/icons/icon.png" width="120" alt="GazaBridge"></p>

<h1 align="center">GazaBridge 🇵🇸</h1>

<p align="center">
برنامج ديسكتوب للتعامل مع قواعد البيانات ونقل البيانات بينها، مكتوب بـ Rust.<br>
MongoDB و PostgreSQL و MySQL / MariaDB و SQLite: تصفّح، استعلامات، تصدير، نسخ، تحويل، تحقق، نسخ احتياطي ومزامنة.
</p>

<p align="center">
<a href="https://github.com/sabahya3/GazaBridge/releases/latest/download/GazaBridge-macos-universal.dmg"><img alt="Download for macOS" src="https://img.shields.io/badge/Download_for-macOS-111111?style=for-the-badge&logo=apple&logoColor=white"></a>
<a href="https://github.com/sabahya3/GazaBridge/releases/latest/download/GazaBridge-Setup-windows-x86_64.exe"><img alt="Download for Windows" src="https://img.shields.io/badge/Download_for-Windows-0078D6?style=for-the-badge&logo=data:image/svg%2bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZmlsbD0iI2ZmZiIgZD0iTTAgMy40IDkuOCAydjkuNUgwek0xMSAxLjggMjQgMHYxMS41SDExek0wIDEyLjVoOS44VjIyTDAgMjAuNnpNMTEgMTIuNWgxM1YyNGwtMTMtMS44eiIvPjwvc3ZnPg=="></a>
<a href="https://github.com/sabahya3/GazaBridge/releases/latest/download/GazaBridge-linux-x86_64.AppImage"><img alt="Download for Linux" src="https://img.shields.io/badge/Download_for-Linux-E95420?style=for-the-badge&logo=linux&logoColor=white"></a>
</p>

<p align="center">
<a href="https://github.com/sabahya3/GazaBridge/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/sabahya3/GazaBridge?style=flat-square&label=version&include_prereleases"></a>
<a href="LICENSE"><img alt="Apache-2.0 license" src="https://img.shields.io/badge/license-Apache--2.0-149954?style=flat-square"></a>
<img alt="Written in Rust" src="https://img.shields.io/badge/made_with-Rust-B7410E?style=flat-square&logo=rust&logoColor=white">
<img alt="Tested on macOS" src="https://img.shields.io/badge/tested_on-macOS-111111?style=flat-square&logo=apple&logoColor=white">
</p>

<p align="center"><a href="README.md">English</a> · <b>العربية</b></p>

GazaBridge بيتوصل بـ MongoDB و PostgreSQL و MySQL / MariaDB و SQLite. تقدر تتصفح البيانات وتعدّلها، وتكتب استعلامات، وتصدّر وتستورد ملفات. وكمان بينسخ قواعد بيانات كاملة، ويحوّل كولكشنز MongoDB لجداول SQL ويرجّعها تاني، ويتأكد إن النسخ كامل، ويعمل نسخ احتياطي، ويفضل يزامن الهدف مع المصدر. كله بيشتغل على جهازك: مفيش حساب، ولا سيرفر، ولا تتبّع (telemetry). الواجهة بالإنجليزي والعربي (من اليمين للشمال).

> [!IMPORTANT]
> **ده الإصدار 0.1.0، إصدار بدري. وكل التجارب اتعملت على macOS بس** (ماك Apple Silicon، macOS 26).
> الكود مكتوب يشتغل على Windows و Linux كمان، و GitHub Actions بيبنيهم، بس لسه محدش شغّل النسخ دي على جهاز Windows أو Linux حقيقي. لو جربت واحدة منهم، يا ريت [تفتح issue](../../issues) وتقول حصل إيه.

## التحميل

من صفحة [**Releases**](../../releases/latest):

| النظام | الملف | التشغيل |
|---|---|---|
| **macOS** 11+ (Apple Silicon و Intel) | [`GazaBridge-macos-universal.dmg`](https://github.com/sabahya3/GazaBridge/releases/latest/download/GazaBridge-macos-universal.dmg) | افتحه واسحب **GazaBridge** لـ Applications. أول مرة اعمل كليك يمين ← **Open**، لأن التطبيق مش موثّق من Apple (notarized). **اتجرّب.** |
| **Windows** 10 / 11 | [`GazaBridge-Setup-windows-x86_64.exe`](https://github.com/sabahya3/GazaBridge/releases/latest/download/GazaBridge-Setup-windows-x86_64.exe) | برنامج التثبيت: افتحه واضغط Next. وموجود كمان كـ `.msi`. **لسه ما اتجربش على Windows حقيقي.** |
| **Linux** (x86_64) | [`GazaBridge-linux-x86_64.AppImage`](https://github.com/sabahya3/GazaBridge/releases/latest/download/GazaBridge-linux-x86_64.AppImage) | اعمله `chmod +x` وشغّله. وموجود كمان كـ `.deb`. محتاج WebKitGTK 4.1. **لسه ما اتجربش على Linux حقيقي.** |

- لو macOS قال إن التطبيق "is damaged and can't be opened"، شغّل الأمر ده مرة واحدة: `xattr -dr com.apple.quarantine /Applications/GazaBridge.app`
- لو Windows طلّع رسالة SmartScreen، اضغط **More info** ← **Run anyway**، لأن البرنامج جديد ومش موقّع.
- شوية مميزات بتستخدم أدوات بتتسطب لوحدها: `mongodump` / `mongorestore` (MongoDB Database Tools) و `pg_dump` / `pg_restore` (PostgreSQL client). البرنامج بيدوّر عليهم، ولو مش موجودين بيقولك تسطبهم إزاي.

## بيعمل إيه

### الاتصالات
- MongoDB (ومنها Atlas و `mongodb+srv://`)، و PostgreSQL، و MySQL، و MariaDB، و SQLite.
- الصق الـ connection string أو املا الفورم، واضغط **Test** قبل الحفظ.
- إعدادات TLS لكل محرك، و**أنفاق SSH** لـ PostgreSQL و MySQL و MongoDB (باسورد أو ملف مفتاح أو SSH agent، مع التحقق من مفتاح السيرفر).
- الباسوردات بتتحفظ في الخزنة الآمنة بتاعة النظام (Keychain على الماك، Credential Manager على Windows، Secret Service على Linux) أو للجلسة بس. عمرها ما بتتكتب في ملفات البرنامج ولا اللوجز ولا في أوامر تشغيل الأدوات.

### المستكشف والمحرر
- شجرة فيها قواعد البيانات والـ schemas والكولكشنز والجداول بعددها.
- البيانات كجدول أو JSON أو شجرة، صفحة صفحة.
- شريط استعلام لـ MongoDB: بنّاء مرئي أو نص بأسلوب mongosh، مع sort و projection و skip و limit، و aggregation pipelines.
- تعديل وإضافة ومسح مستندات وصفوف، مع معاينة للتغيير قبله وسجل (audit) بعده.

### Query Studio
- استعلامات MongoDB و SQL، مع التاريخ، والاستعلامات المحفوظة، والـ parameters، و**Cancel** و**Explain**.

### الملفات
- **تصدير** لـ JSON و JSONL و BSON و CSV و Excel (`.xlsx`)، ومع gzip لو عايز. التصدير بيمشي على الاستعلام الحالي (filter و projection و sort و skip و limit).
- **استيراد** من JSON و JSONL و BSON و CSV. السجلات البايظة بتتعدّى أو بتتحط في ملف quarantine لوحدها، مع رقم السطر.

### النسخ والتحويل
- **MongoDB ← MongoDB**: قواعد بيانات كاملة، مع الـ indexes والـ views وإعدادات الكولكشنز وقواعد الـ validation.
- **MongoDB ← PostgreSQL أو MySQL** بأربع طرق: المستند كله في عمود JSON، أو أعمدة **مفرودة** (`address.city` ← `address_city`)، أو **جداول فرعية** للـ arrays (normalize)، أو خليط من أعمدة بأنواعها مع المستند كله. الـ ObjectId بيفضل ObjectId، ومفيش حاجة بتضيع في السكوت: القيم اللي مش راكبة على الأعمدة بتتحط في عمود `_extra`.
- **PostgreSQL أو MySQL ← MongoDB**: مستند لكل صف، والجداول الفرعية بتتحط جوه المستند كـ arrays أو بتفضل references. والجداول اللي اتعملت بوضع flatten أو normalize بترجع **بنفس شكل المستند الأصلي** (objects جوه بعض، arrays، ObjectIds، أرقام 32 بت)، حتى الصفوف اللي اتضافت بعدين بـ SQL.
- **نسخ جداول** بين PostgreSQL و MySQL (وجوه كل واحد فيهم).
- كل نسخ بيتعمله خطة الأول: بتشوف الجداول والأعمدة والأنواع وصفوف عيّنة والأوامر بالظبط قبل ما أي حاجة تتكتب. و**Dry run** بيمشي في كل الخطوات من غير ما يكتب حاجة.
- إيقاف مؤقت، وكمّل بعدين (حتى بعد ما تقفل البرنامج)، وإعادة محاولة، و quarantine للسجلات اللي بتفشل.

### التحقق والمراقبة والتقارير
- بعد النسخ: تحقق بالعدد، وبالمفاتيح، و**تحقق عميق** (checksums) بيسمّي كل سجل مختلف.
- صفحة **Monitor** فيها سجلات في الثانية، والوقت الباقي، وإيه اللي مبطّأ النسخ.
- **تقارير** لكل عملية بـ HTML أو Markdown أو JSON أو CSV.

### النسخ الاحتياطي والمزامنة
- نسخ احتياطي واسترجاع بصيغة GazaBridge (فيها manifest و SHA-256) لـ MongoDB و PostgreSQL و MySQL، أو بـ `mongodump` / `pg_dump`. وبيتعمل نسخة احتياطية لوحده قبل ما نسخ يستبدل بيانات موجودة.
- **مزامنة مستمرة** بعد النسخ: change streams في MongoDB، أو متابعة عمود وقت (timestamp) لأي مصدر. إيقاف وكمّل ووقف من صفحة **Sync Jobs**.

## قواعد البيانات المدعومة

| قاعدة البيانات | الإصدارات اللي اتجربت |
|---|---|
| MongoDB | 6.0، 7.0، 8.0 |
| PostgreSQL | 13، 14، 15، 16، 17 |
| MySQL | 8.0، 8.4 |
| MariaDB | 10.11، 11.4 |
| SQLite | 3.46 |

كل موصّل بيعدّي مجموعة اختبارات موحّدة (contract suite) على السيرفر الحقيقي. الخدمات السحابية (Atlas و Neon و Supabase…) بتستخدم نفس الموصّلات، بس ما اتجربتش بحسابات حقيقية.

## اتجرّب إزاي

- **كل التجارب اتعملت على macOS** (Apple Silicon). التطبيق نفسه اتبنى واستخدم على ماك بس.
- 336 اختبار أوتوماتيك. أغلبهم بيشتغلوا على **سيرفرات قواعد بيانات حقيقية في Docker** (Testcontainers) مش mocks: الـ contract suite لكل موصّل، والنسخ والتحويل في كل الاتجاهات، والنسخ الاحتياطي والاسترجاع، والمزامنة، وأنفاق SSH، والتحقق.
- اختبار لفّة كاملة بيبعت 300 مستند فيهم كل أنواع BSON المشهورة (نص عربي وإيموجي، Decimal128، تواريخ قبل 1970، أرقام 64 بت، بيانات binary، objects و arrays جوه بعض، null وحقل مش موجود) من MongoDB ← PostgreSQL ← MySQL ← MariaDB ← PostgreSQL ← MongoDB، ويتأكد من كل قيمة في الطريق.
- الـ Dry run متأكدين باختبارات إنه مش بيكتب أي حاجة.
- `cargo fmt`، و `cargo clippy` والتحذيرات بتتعامل كأخطاء، و `cargo audit`.

## اللي لسه ما اتعملش

- **Windows و Linux**: GitHub Actions بيبنيهم، بس ما اتجربوش على جهاز حقيقي. وعلى Windows، الدخول بالـ SSH agent لسه مش مدعوم (الباسورد وملف المفتاح شغالين).
- **التوزيع**: تطبيق الماك مش موثّق من Apple (notarization)، وبرنامج تثبيت Windows مش موقّع، ومفيش تحديثات أوتوماتيك.
- **صفحات لسه مكتوب عليها "قريبًا"**: Schema Studio، والتحويلات (Transformations)، والجدولة (Schedules). يعني لسه مفيش مهام أو نسخ احتياطي بمواعيد.
- **المزامنة**: مفيش مزامنة في الاتجاهين، ولا PostgreSQL logical replication ولا MySQL binlog (بيستخدم المتابعة بالوقت بدالهم، ودي مش بتشوف المسح).
- **الملفات**: قراءة ملفات Excel، و CSV بترميز غير UTF-8، و Parquet / Arrow.
- **الأدوات**: `pg_dump` / `pg_restore` متجربين في unit tests بس، و `mysqldump` مش مدعوم (النسخ الاحتياطي لـ MySQL بصيغة GazaBridge).
- **SSH**: ملفات مفاتيح RSA (مفاتيح RSA شغالة من خلال الـ SSH agent)، و jump hosts، و `mongodb+srv` من خلال نفق.
- **الواجهة**: جدول البيانات بالصفحات مش virtualized، ومحرر Query Studio من غير تلوين أو إكمال تلقائي.
- لسه مفيش نسخة سطر أوامر (CLI).

القايمة بالتفصيل في [docs/STATUS.md](docs/STATUS.md)، والخطة في [docs/roadmap.md](docs/roadmap.md).

## البناء من الكود

محتاج [Rust](https://rustup.rs) 1.90 أو أحدث مع target الـ WebAssembly، و [Trunk](https://trunkrs.dev)، و Node.js (عشان Tauri CLI):

```bash
rustup target add wasm32-unknown-unknown
cargo install trunk --locked
cd apps/desktop/src-tauri
npx @tauri-apps/cli@2.9 dev      # تشغيل وانت بتطوّر
npx @tauri-apps/cli@2.9 build    # برامج تثبيت للنظام اللي انت عليه
```

على Linux، سطّب المكتبات دي الأول:

```bash
sudo apt install libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libdbus-1-dev pkg-config
```

### تطبيق ماك

```bash
scripts/build-mac-app.sh        # GazaBridge.app
scripts/build-mac-app.sh dmg    # ومعاه .dmg
```

عشان التوقيع يفضل واحد بين كل بناء والتاني (فالماك ما يسألكش تاني على الباسوردات المحفوظة)، حط الـ SHA-1 بتاع شهادة التوقيع بتاعتك (من `security find-identity -v -p codesigning`) في ملف `.signing-identity` في أول فولدر في المشروع. git بيتجاهله.

### إصدار جديد

اعمل push لـ tag بالإصدار، و GitHub Actions هيبني نسخ macOS و Windows و Linux ويحطهم في Releases:

```bash
git tag v0.1.0 && git push origin v0.1.0
```

### الاختبارات

```bash
cargo test --workspace                       # الاختبارات السريعة
cargo test --workspace -- --include-ignored  # ومعاها اللي محتاجة Docker
```

## ملفات المشروع

| المسار | إيه هو |
|---|---|
| `apps/desktop/src-tauri` | الجزء الخلفي من التطبيق (Tauri 2): الأوامر وحالة التطبيق |
| `apps/desktop/ui` | الواجهة (Leptos، بتتحول لـ WebAssembly)، بالإنجليزي والعربي |
| `crates/gazabridge-domain` | القيم والأنواع والتحويل اللي بيحسب الفقد، وإعدادات الاتصالات، وحالات المهام |
| `crates/gazabridge-plugin-sdk` | واجهة الموصّلات: قراءة وكتابة وتغيير schema واستعلامات ومتابعة تغييرات |
| `crates/gazabridge-connector-*` | موصّلات MongoDB و PostgreSQL و MySQL / MariaDB و SQLite |
| `crates/gazabridge-bson` | BSON ⇄ قيم GazaBridge |
| `crates/gazabridge-mapping` | مستندات ⇄ جداول، وإعادة بناء المستندات من الجداول |
| `crates/gazabridge-migration` | محرك النسخ: الـ pipeline والـ checkpoints وإعادة المحاولة والتحقق والمزامنة |
| `crates/gazabridge-files` | قراءة وكتابة الملفات (JSON و JSONL و BSON و CSV و Excel) |
| `crates/gazabridge-query` | بنّاء استعلامات MongoDB وقارئ نص mongosh |
| `crates/gazabridge-ssh` | أنفاق SSH |
| `crates/gazabridge-secrets` | الأسرار في Keychain والجلسة والبيئة، وإخفاؤها من اللوجز |
| `crates/gazabridge-storage` | قاعدة بيانات SQLite بتاعة التطبيق (الاتصالات، المهام، التاريخ) |
| `crates/gazabridge-application` | مميزات التطبيق، مشتركة بين تطبيق الديسكتوب و CLI بعدين |
| `docs/` | الحالة، والخطة، والمعمارية، والقرارات (ADRs)، ونموذج التهديدات |

## الأمان

بيانات الدخول عمرها ما بتتحط في ملفات GazaBridge: الاتصالات المحفوظة بتشاور بس على الخزنة الآمنة بتاعة النظام، وقاعدة بيانات البرنامج بترفض أي باسورد مكتوب كنص. اللوجز ورسايل الأخطاء بتخفي الباسوردات. والأدوات زي `pg_dump` بتاخد الباسورد من ملف مؤقت خاص، عمره ما بيتكتب في سطر الأوامر. ومفيش telemetry. اقرا [SECURITY.md](SECURITY.md) و [docs/threat-model.md](docs/threat-model.md).

## الترخيص

[Apache-2.0](LICENSE).
