const express = require("express");
const multer = require("multer");
const fs = require("fs-extra");
const path = require("path");
const moment = require("moment");
const xlsx = require("xlsx");
const https = require("https");
const cors = require("cors");
const { Console } = require("console");
const mysql = require('mysql2/promise');
const jwt = require("jsonwebtoken");
const cookieParser = require("cookie-parser"); 
require("dotenv").config();



// Definir directorio base para todos los archivos
const baseDir = path.join(__dirname, "files");
const debugLogDir = path.join(baseDir, "logs");

// Asegurar que el directorio de logs existe
fs.ensureDirSync(debugLogDir);

// Configurar logger personalizado para información más detallada
const logFile = fs.createWriteStream(path.join(debugLogDir, 'app-debug.log'), { flags: 'a' });
const logConsole = new Console({ stdout: logFile, stderr: logFile });

// Log de inicio de servidor
const startupLog = `\n===============================\n📋 SERVIDOR INICIADO: ${new Date().toISOString()}\n===============================\n`;
logConsole.log(startupLog);
console.log(startupLog);

// Configuración de la base de datos
const dbConfig = {
    host: "localhost",
    user: "admin",
    password: "Usuario19.",
    database: "rates_db",
};

// Cargar certificados SSL
const options = {
    key: fs.readFileSync("/etc/letsencrypt/live/nwfg.net/privkey.pem"),
    cert: fs.readFileSync("/etc/letsencrypt/live/nwfg.net/fullchain.pem")
};

const app = express();
app.use(cookieParser()); // 👈 esta es la línea que necesitas
const PORT = process.env.PORT || 3001;

// Middleware para permitir JSON con límite aumentado para archivos grandes
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Middleware de logging para todas las peticiones
app.use((req, res, next) => {
    const timestamp = moment().format("YYYY-MM-DD HH:mm:ss");
    console.log(`[${timestamp}] 📌 ${req.method} ${req.path}`);
    logConsole.log(`[${timestamp}] 📌 ${req.method} ${req.path}`);
    next();
});

// Habilitar CORS con configuración mejorada
app.use(cors({
    origin: ["https://www.nwfg.net", "https://nwfg.net", "http://localhost:3000"],
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
    credentials: true
}));

// Configurar almacenamiento en memoria con Multer
const storage = multer.memoryStorage();
const upload = multer({ 
    storage: storage,
    limits: { fileSize: 10 * 1024 * 1024 } // Límite de 10MB
});

// 📌 Ruta para guardar las columnas seleccionadas en el paso 2
app.post("/columns/selected", async (req, res) => {
    try {
        const { supplier, selectedColumns } = req.body;
        
        if (!supplier || !selectedColumns || !Array.isArray(selectedColumns)) {
            console.error("❌ Error: Datos incompletos o inválidos");
            return res.status(400).json({ 
                success: false, 
                error: "Se requiere proveedor y columnas seleccionadas en formato correcto" 
            });
        }
        
        // 📌 Obtener la fecha actual
        const date = moment().format("YYYY-MM-DD");
        const time = moment().format("HH:mm:ss");
        
        // 📂 Definir directorios y archivos necesarios
        const logDir = path.join(baseDir, "logs");
        const logFilePath = path.join(logDir, `${date}.log`);
        const tempDir = path.join(baseDir, "temp");
        const selectedColumnsFile = path.join(tempDir, `selected_columns_${supplier}.json`);
        
        // ✅ Crear directorios si no existen
        await fs.ensureDir(logDir);
        await fs.ensureDir(tempDir);
        
        // 📌 Guardar las columnas seleccionadas para usarlas en el paso 3
        await fs.writeJson(selectedColumnsFile, {
            supplier,
            columns: selectedColumns,
            timestamp: new Date().toISOString()
        }, { spaces: 2 });
        
        // 📌 Actualizar el log con las columnas seleccionadas
        const logEntry = `
🔄 [${time}] Paso 2 completado
🏢 Proveedor: ${supplier}
✅ Columnas seleccionadas: ${selectedColumns.length} (${selectedColumns.join(", ")})
`;
        await fs.appendFile(logFilePath, logEntry);
        
        console.log(`✅ Columnas seleccionadas guardadas para ${supplier}: ${selectedColumns.length}`);
        logConsole.log(`✅ Columnas seleccionadas guardadas para ${supplier}: ${selectedColumns.length}`);
        
        res.json({ 
            success: true, 
            message: `${selectedColumns.length} columnas seleccionadas guardadas correctamente` 
        });
        
    } catch (error) {
        console.error("❌ Error al guardar columnas seleccionadas:", error);
        logConsole.error("❌ Error al guardar columnas seleccionadas:", error);
        res.status(500).json({ success: false, error: `Error interno: ${error.message}` });
    }
});

// 📌 Ruta para obtener las columnas seleccionadas para el paso 3
app.get("/columns/selected/:supplier", async (req, res) => {
    try {
        const supplier = req.params.supplier;
        if (!supplier) {
            return res.status(400).json({ success: false, error: "Se requiere especificar un proveedor" });
        }
        
        const tempDir = path.join(__dirname, "files", "temp");
        const selectedColumnsFile = path.join(tempDir, `selected_columns_${supplier}.json`);
        
        if (!await fs.pathExists(selectedColumnsFile)) {
            return res.status(404).json({ 
                success: false, 
                error: "No se encontraron columnas seleccionadas para este proveedor" 
            });
        }
        
        const data = await fs.readJson(selectedColumnsFile);
        
        console.log(`📌 Devolviendo columnas seleccionadas para ${supplier}: ${data.columns.length}`);
        
        res.json({ 
            success: true, 
            selectedColumns: data.columns, 
            timestamp: data.timestamp
        });
        
    } catch (error) {
        console.error("❌ Error al obtener columnas seleccionadas:", error);
        logConsole.error("❌ Error al obtener columnas seleccionadas:", error);
        res.status(500).json({ success: false, error: `Error interno: ${error.message}` });
    }
});

// 📌 Ruta para subir archivos
app.post("/file", upload.single("file"), async (req, res) => {
    try {
        const file = req.file;
        const supplier = req.body.supplier;
        // Recuperar y parsear selectedColumns si existen
        const selectedColumns = req.body.selectedColumns ? JSON.parse(req.body.selectedColumns) : [];

        console.log("📌 Columnas seleccionadas recibidas en el backend:", selectedColumns);
        logConsole.log("📌 Columnas seleccionadas recibidas en el backend:", selectedColumns);

        if (!file || !supplier) {
            console.error("❌ Error: Falta el archivo o el proveedor");
            return res.status(400).json({ success: false, error: "Falta el archivo o el proveedor (supplier)." });
        }

        // 📌 Validar tipo de archivo
        const validFileTypes = ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.ms-excel"];
        if (!validFileTypes.includes(file.mimetype)) {
            console.error(`❌ Error: Tipo de archivo inválido - ${file.mimetype}`);
            return res.status(400).json({ success: false, error: "Tipo de archivo inválido. Solo se permiten archivos Excel (.xlsx o .xls)" });
        }

        // 📌 Obtener la fecha actual
        const date = moment().format("YYYY-MM-DD");
        const time = moment().format("HH:mm:ss");

        // 📂 Definir rutas para almacenamiento de archivos
        const supplierDir = path.join(baseDir, supplier);
        const dateDir = path.join(supplierDir, date);
        const filePath = path.join(dateDir, file.originalname);
        
        // 📂 Definir rutas para logs (ahora dentro de files/logs)
        const logDir = path.join(baseDir, "logs");
        const logFilePath = path.join(logDir, `${date}.log`);

        // ✅ Crear las carpetas necesarias si no existen
        await fs.ensureDir(dateDir);
        await fs.ensureDir(logDir);

        // 📌 Guardar el archivo
        await fs.writeFile(filePath, file.buffer);

        // 📌 Leer el archivo Excel para extraer las columnas
        let workbook, sheetName, worksheet, jsonData;
        try {
            workbook = xlsx.read(file.buffer, { type: "buffer" });
            sheetName = workbook.SheetNames[0];
            worksheet = workbook.Sheets[sheetName];
            jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });
        } catch (excelError) {
            console.error("❌ Error al leer el archivo Excel:", excelError);
            return res.status(400).json({ 
                success: false, 
                error: "Error al leer el archivo Excel. Verifica que el formato sea correcto." 
            });
        }

        // 📌 Extraer nombres de columnas
        const columns = jsonData[0] || [];

        if (columns.length === 0) {
            console.error("❌ Error: No se encontraron columnas en el archivo");
            return res.status(400).json({ 
                success: false, 
                error: "No se encontraron columnas en el archivo. Verifica el formato del Excel." 
            });
        }

        // 📌 Extraer muestras de datos para cada columna (hasta 5 filas)
        const samples = {};
        if (columns.length > 0 && jsonData.length > 1) {
            columns.forEach((col, colIndex) => {
                samples[col] = [];
                for (let i = 1; i < Math.min(jsonData.length, 6); i++) {
                    if (jsonData[i][colIndex] !== undefined) {
                        samples[col].push(jsonData[i][colIndex]);
                    }
                }
            });
        }

        // 📌 Extraer TODAS las filas de datos completas
        const allRows = [];
        if (jsonData.length > 1) {
            // Comenzar desde 1 para omitir la fila de encabezados
            for (let i = 1; i < jsonData.length; i++) {
                const row = {};
                columns.forEach((col, colIndex) => {
                    row[col] = jsonData[i][colIndex] !== undefined ? jsonData[i][colIndex] : null;
                });
                allRows.push(row);
            }
        }

        // Verificar si no hay filas de datos válidas
        if (allRows.length === 0) {
            console.warn("⚠️ El archivo no contiene filas válidas para procesar.");
            logConsole.warn("⚠️ El archivo no contiene filas válidas para procesar.");
            return res.status(400).json({
                success: false,
                error: "El archivo no contiene ninguna fila de datos válida."
            });
        }

        console.log(`📊 Total de filas extraídas del Excel: ${allRows.length}`);
        logConsole.log(`📊 Total de filas extraídas del Excel: ${allRows.length}`);

        // 📂 Guardar todas las filas en un archivo temporal
        const tempDir = path.join(baseDir, "temp");
        await fs.ensureDir(tempDir);
        const rowsFile = path.join(tempDir, `rows_${supplier}_${date}.json`);
        await fs.writeJson(rowsFile, {
            supplier,
            fileOriginalName: file.originalname,
            totalRows: allRows.length,
            rows: allRows,
            timestamp: new Date().toISOString()
        }, { spaces: 2 });

        // 📌 Actualizar el log con información de las filas extraídas
        await fs.appendFile(logFilePath, `📊 Filas totales extraídas del Excel: ${allRows.length}\n`);

        // 📌 Guardar log con detalles y columnas seleccionadas
        const logEntry = `
🗂️ [${time}] 
📄 Archivo: ${file.originalname} | 🏢 Proveedor: ${supplier}
📊 Columnas totales: ${columns.length} (${columns.join(", ")})
✅ Columnas seleccionadas: ${selectedColumns.length > 0 ? selectedColumns.join(", ") : "⏳ Aún no seleccionadas"}
`;
        
        await fs.appendFile(logFilePath, logEntry);

        console.log(`✅ Archivo subido: ${file.originalname} - Columnas: ${columns.length}`);
        logConsole.log(`✅ Archivo subido: ${file.originalname} - Columnas: ${columns.length}`);

        // 📌 Responder con las columnas extraídas y muestras de datos
        res.json({ 
            success: true, 
            message: "Archivo subido y guardado correctamente.", 
            columns,
            samples,
            rowCount: allRows.length, // Añadimos el conteo total de filas
            selectedColumns // Devolver también las columnas seleccionadas para verificación
        });
    } catch (error) {
        console.error("❌ Error al subir archivo:", error);
        logConsole.error("❌ Error al subir archivo:", error);
        res.status(500).json({ success: false, error: `Error interno del servidor: ${error.message}` });
    }
});

// 📌 Ruta para obtener columnas del backend (necesaria para el frontend)
app.get("/columns", async (req, res) => {

    try {
        // Puedes reemplazar esto con una consulta a la base de datos real si lo necesitas
        const columns = [
            "Rate_ID", "SPL_Utility_Name", "Product_Name", "Rate", "ETF", 
            "MSF", "duracion_rate", "Company_DBA_Name","Last_Updated", "SPL"
        ];
        
        console.log("📌 Devolviendo columnas de base de datos:", columns.length);
        res.json({ success: true, columns });
    } catch (error) {
        console.error("❌ Error obteniendo columnas:", error);
        logConsole.error("❌ Error obteniendo columnas:", error);
        res.status(500).json({ success: false, error: "Error interno del servidor." });
    }
});

// 📌 Ruta para mapear columnas y guardar datos
app.post("/map-columns", async (req, res) => {
    let connection = null;
    
    try {
        const { supplier, columnMapping, rows, selectedColumns, headers } = req.body;

        if (!supplier || !columnMapping || !rows || rows.length === 0) {
            console.error("❌ Error: Faltan datos necesarios para el mapeo");
            return res.json({
                success: false,
                message: "Faltan datos necesarios (proveedor, mapping o filas)",
                insertedRows: 0
            });
        }

        console.log("📌 Supplier recibido:", supplier);
        console.log("📌 Columnas seleccionadas en /map-columns:", selectedColumns?.length || 0);
        console.log("📌 Mapping de columnas recibido:", Object.keys(columnMapping).length);
        console.log(`📌 Filas recibidas: ${rows.length}`);

        logConsole.log("📌 Supplier recibido:", supplier);
        logConsole.log("📌 Columnas seleccionadas en /map-columns:", selectedColumns);
        logConsole.log("📌 Mapping de columnas recibido:", columnMapping);
        logConsole.log(`📌 Filas recibidas: ${rows.length}`);

        // 💡 Validar si hay filas realmente válidas ANTES de tocar la base de datos
        const validRows = rows.filter(row => {
            if (row.Rate !== undefined && row.Rate !== null) {
                let r = String(row.Rate).replace(",", ".");
                r = parseFloat(r);
                return !isNaN(r) && r >= 0;
            }
            return false;
        });

        if (validRows.length === 0) {
            const msg = "🛑 No se encontraron filas válidas para insertar. Se aborta la operación sin tocar la base de datos.";
            console.warn(msg);
            logConsole.warn(msg);
            return res.status(400).json({
                success: false,
                message: msg,
                insertedRows: 0
            });
        }

        // 📌 Obtener la fecha actual
        const date = moment().format("YYYY-MM-DD");
        const time = moment().format("HH:mm:ss");
        const timestamp = moment().format("YYYYMMDD_HHmmss");

        // 📂 Definir directorio para logs
        const logDir = path.join(baseDir, "logs");
        const mappingLogPath = path.join(logDir, `${date}.log`);

        // ✅ Crear directorio de logs si no existe
        await fs.ensureDir(logDir);

        // 📌 Guardar detalles del mapping en el log
        const mappingLogEntry = `
🗂️ [${time}] 📄 Archivo: ${supplier}
📊 Columnas totales en el archivo: ${headers?.length || 0} (${headers ? headers.join(", ") : "N/A"})
✅ Columnas seleccionadas por el usuario: ${selectedColumns?.length || 0} (${selectedColumns ? selectedColumns.join(", ") : "N/A"})
🔄 Mapping aplicado: ${JSON.stringify(columnMapping, null, 2)}
📊 Filas procesadas: ${rows.length}
`;
        await fs.appendFile(mappingLogPath, mappingLogEntry);

        // 📌 NUEVA FUNCIONALIDAD: Conexión a la base de datos MySQL
        try {
            console.log("🔄 Intentando conectar a la base de datos con la configuración:");
            console.log(dbConfig);
            logConsole.log("🔄 Intentando conectar a la base de datos con la configuración:");
            logConsole.log(dbConfig);
        
            // Crear conexión a la base de datos
            connection = await mysql.createConnection(dbConfig);
        
            console.log("✅ Conexión a la base de datos establecida");
            logConsole.log("✅ Conexión a la base de datos establecida")

            // Crear tabla de respaldo con timestamp
            const backupTable = `Rates_backup_${timestamp}`;
            await connection.query(`CREATE TABLE ${backupTable} LIKE Rates`);
            await connection.query(`INSERT INTO ${backupTable} SELECT * FROM Rates`);
            console.log(`✅ Backup creado: ${backupTable}`);
            logConsole.log(`✅ Backup creado: ${backupTable}`);
            
            // Guardar información del backup en el log
            await fs.appendFile(mappingLogPath, `\n📦 Backup creado: ${backupTable}\n`);

            // Eliminar registros del proveedor seleccionado
            console.log(`🔄 Eliminando registros previos de SPL: ${supplier}`);
            logConsole.log(`🔄 Eliminando registros previos de SPL: ${supplier}`);
            const [deleteResult] = await connection.query("DELETE FROM Rates WHERE SPL = ?", [supplier]);
            console.log(`✅ Registros eliminados: ${deleteResult.affectedRows}`);
            logConsole.log(`✅ Registros eliminados: ${deleteResult.affectedRows}`);
            
            // Guardar información de eliminación en el log
            await fs.appendFile(mappingLogPath, `\n🗑️ ${deleteResult.affectedRows} registros previos de ${supplier} eliminados\n`);

            // Construir query de inserción dinámicamente
            const dbColumns = Object.values(columnMapping); // Columnas destino en la BD
            
            // Asegurar que SPL está incluido en las columnas
            if (!dbColumns.includes("SPL")) {
                dbColumns.push("SPL");
            }

            // 📌 Extraer duracion_rate desde Product_Name para Clean Sky (SPL = 'cs')
            if (supplier === 'cs') {
                console.log("📌 Procesando datos de Clean Sky: extrayendo duracion_rate de Product_Name");
                logConsole.log("📌 Procesando datos de Clean Sky: extrayendo duracion_rate de Product_Name");
                
                // Añadir duracion_rate a las columnas si no está presente
                if (!dbColumns.includes("duracion_rate")) {
                    dbColumns.push("duracion_rate");
                }
                
                // Extraer duracion_rate de cada fila
                for (const row of rows) {
                    if (row.Product_Name) {
                        // Buscar el primer número en Product_Name (por ejemplo, "Eco Rewards 12" -> 12)
                        const match = row.Product_Name.match(/\d+/);
                        row.duracion_rate = match ? parseInt(match[0], 10) : null;
                        
                        console.log(`📌 Extraído duracion_rate: ${row.duracion_rate} de Product_Name: "${row.Product_Name}"`);
                    } else {
                        row.duracion_rate = null;
                    }
                }
                
                await fs.appendFile(mappingLogPath, `\n📊 Extraída duracion_rate automáticamente para ${rows.length} filas de Clean Sky\n`);
            }

            // Crear placeholders para la query (?, ?, ?)
            const placeholders = Array(dbColumns.length).fill("?").join(", ");
            
            // Construir la query de inserción
            const insertQuery = `INSERT INTO Rates (${dbColumns.join(", ")}) VALUES (${placeholders})`;
            
            console.log("📝 Query de inserción preparada:", insertQuery);
            logConsole.log("📝 Query de inserción preparada:", insertQuery);
            
            // Log detallado de los datos a insertar
            const insertionLogPath = path.join(logDir, `${date}_insertion_data.log`);
            await fs.writeFile(insertionLogPath, `\n=== DATOS A INSERTAR (${time}) ===\n\n`);
            
            // Insertar filas en la base de datos
            let insertedCount = 0; // Asegurar que insertedCount está definido

            try {
                for (const row of rows) {
                    try {
                        // 🔄 LIMPIAR Y CONVERTIR VALORES ANTES DE INSERTAR
                
                        // 🔹 Limpiar la columna Rate eliminando cualquier carácter que no sea número o punto decimal
                        if (row.Rate !== undefined && row.Rate !== null) {
                            row.Rate = String(row.Rate).replace(",", ".");
                            row.Rate = parseFloat(row.Rate); // Convertir a número después del reemplazo
                        }
                
                        // 🔹 Si después de la limpieza Rate no es un número válido, lo dejamos como NULL
                        if (isNaN(row.Rate) || row.Rate < 0) {
                            console.warn(`⚠️ Fila omitida por Rate inválido: ${JSON.stringify(row)}`);
                            continue; // Omitir esta fila y pasar a la siguiente
                        }
                
                        // 🔹 Preparar valores para la inserción en la base de datos
                        const values = dbColumns.map(col => {
                            if (col === "SPL" && (!row[col] || row[col] === "")) {
                                return supplier;
                            }
                            return row[col] !== undefined && row[col] !== null && row[col] !== "" ? row[col] : null;
                        });
                
                        // 🔹 Insertar en la base de datos
                        await connection.query(insertQuery, values);
                        insertedCount++;
                
                    } catch (insertError) {
                        console.error(`❌ Error insertando fila #${insertedCount + 1}:`, insertError);
                        logConsole.error(`❌ Error insertando fila #${insertedCount + 1}:`, insertError);
                    }
                }              
            
                console.log(`✅ ${insertedCount} filas insertadas correctamente en Rates`);
                logConsole.log(`✅ ${insertedCount} filas insertadas correctamente en Rates`);
            
                // ✅ Obtener el nombre original del archivo desde el archivo temporal
                const originalFilePath = path.join(baseDir, "temp", `rows_${supplier}_${date}.json`);
                let originalFileName = `${supplier}_${date}.xlsx`; // valor por defecto
                
                try {
                    if (await fs.pathExists(originalFilePath)) {
                        const fileData = await fs.readJson(originalFilePath);
                        originalFileName = fileData.fileOriginalName || originalFileName;
                        console.log(`📄 Nombre original del archivo recuperado: ${originalFileName}`);
                    }
                } catch (fileReadError) {
                    console.error("⚠️ Error al leer el nombre original del archivo:", fileReadError);
                    // Continuamos con el nombre por defecto si hay error
                }
                
                // Extraer username del request body
                let username = "Desconocido";

                try {
                const token = req.cookies?.token; // la cookie se llama 'token'
                if (token) {
                    try {
                        const decoded = jwt.verify(token, process.env.JWT_SECRET);
                        
                        console.log("🧠 JWT recibido:");
                        console.log(JSON.stringify(decoded, null, 2)); // 👈 Log completo con formato

                        // Extraer campos útiles
                        const { nombre, role, centro } = decoded;
                        username = nombre || "Desconocido";

                        console.log(`🔐 Usuario: ${nombre}`);
                        console.log(`🔐 Rol: ${role}`);
                        console.log(`🏢 Centro: ${centro}`);

                    } catch (err) {
                        console.warn("⚠️ No se pudo decodificar el token:", err.message);
                    }
                } else {
                    console.warn("⚠️ No se encontró token en la cookie.");
                }
                } catch (err) {
                console.warn("⚠️ No se pudo decodificar el token:", err.message);
                }
                
                // Enviar la respuesta asegurando que insertedCount esté presente
                return res.json({
                    success: true,
                    message: `Datos procesados correctamente: ${insertedCount} filas insertadas.`,
                    insertedRows: insertedCount
                });
            
            } catch (error) {
                console.error("❌ Error en la inserción de datos:", error);
                logConsole.error("❌ Error en la inserción de datos:", error);
            
                return res.status(500).json({
                    success: false,
                    message: `Error interno del servidor: ${error.message}`
                });
            }
            
            console.log(`✅ ${insertedCount} filas insertadas correctamente en Rates`);
            logConsole.log(`✅ ${insertedCount} filas insertadas correctamente en Rates`);
            
            // Guardar resumen de la inserción en el log principal
            await fs.appendFile(mappingLogPath, `\n✅ ${insertedCount} filas insertadas en la base de datos\n`);
            
            // Registrar en el log si hubo discrepancia entre filas procesadas y insertadas
            if (insertedCount !== rows.length) {
                const message = `⚠️ Advertencia: Solo se insertaron ${insertedCount} de ${rows.length} filas`;
                console.warn(message);
                logConsole.warn(message);
                await fs.appendFile(mappingLogPath, `\n${message}\n`);
            }
            
            } catch (dbError) {
                console.error("❌ Error en operación de base de datos:", dbError);
                logConsole.error("❌ Error en operación de base de datos:", dbError);
                await fs.appendFile(mappingLogPath, `\n❌ ERROR DE BASE DE DATOS: ${dbError.message}\n`);
            
                // Si hay error de DB, propagarlo para que se maneje en el catch general
                throw new Error(`Error de base de datos: ${dbError.message}`);
            }
            
            // Verificar si se insertaron filas antes de considerar la operación como exitosa
            if (insertedCount === 0) {
                const msg = "El archivo fue procesado, pero no se insertó ninguna fila. Verifica el mapeo o los datos.";
                console.warn(`📭 ${msg}`);
                logConsole.warn(`📭 ${msg}`);

                return res.status(400).json({
                    success: false,
                    message: msg,
                    insertedRows: 0
                });
            }
            
            res.json({
                success: true,
                message: `Datos procesados correctamente: ${insertedCount} filas insertadas.`,
                insertedRows: insertedCount
            });
            
            } catch (error) {
                console.error("❌ Error en /map-columns:", error);
                logConsole.error("❌ Error en /map-columns:", error);
                res.status(500).json({ 
                    success: false,
                    message: `Error interno del servidor: ${error.message}`
                });
            } finally {
                // Cerrar la conexión a la base de datos si está abierta
                if (connection) {
                    try {
                        await connection.end();
                        console.log("📌 Conexión a la base de datos cerrada");
                        logConsole.log("📌 Conexión a la base de datos cerrada");
                    } catch (closeError) {
                        console.error("❌ Error al cerrar la conexión:", closeError);
                        logConsole.error("❌ Error al cerrar la conexión:", closeError);
                    }
                }
            }
            
});

// 📌 Ruta para verificar el estado del servidor
app.get("/health", (req, res) => {
    res.json({ 
        status: "OK", 
        timestamp: new Date().toISOString(),
        version: "1.0.0" 
    });
});

// 📌 Ruta para obtener todas las filas del archivo subido
app.get("/rows/:supplier", async (req, res) => {
    try {
        const supplier = req.params.supplier;
        if (!supplier) {
            return res.status(400).json({ success: false, error: "Se requiere especificar un proveedor" });
        }
        
        const tempDir = path.join(baseDir, "temp");
        const date = moment().format("YYYY-MM-DD");
        const rowsFile = path.join(tempDir, `rows_${supplier}_${date}.json`);
        
        if (!await fs.pathExists(rowsFile)) {
            return res.status(404).json({ 
                success: false, 
                error: "No se encontraron datos para este proveedor. Asegúrate de haber subido un archivo primero." 
            });
        }
        
        const data = await fs.readJson(rowsFile);
        
        console.log(`📊 Devolviendo ${data.totalRows} filas para ${supplier}`);
        logConsole.log(`📊 Devolviendo ${data.totalRows} filas para ${supplier}`);
        
        res.json({ 
            success: true, 
            supplier: data.supplier,
            fileName: data.fileOriginalName,
            rowCount: data.totalRows,
            rows: data.rows,
            timestamp: data.timestamp
        });
        
    } catch (error) {
        console.error("❌ Error al obtener filas:", error);
        logConsole.error("❌ Error al obtener filas:", error);
        res.status(500).json({ success: false, error: `Error interno: ${error.message}` });
    }
});

// 📌 Manejador de errores
app.use((err, req, res, next) => {
    console.error("❌ Error no controlado:", err);
    logConsole.error("❌ Error no controlado:", err);
    res.status(500).json({ success: false, error: "Error interno del servidor" });
});

// 📌 Crear servidor HTTPS
https.createServer(options, app).listen(PORT, "0.0.0.0", () => {
    const startupMessage = `🔐 Servidor seguro corriendo en https://nwfg.net:${PORT} - ${new Date().toISOString()}`;
    console.log(startupMessage);
    logConsole.log(startupMessage);
});