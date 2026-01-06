const express = require("express");
const multer = require("multer");
const fs = require("fs-extra");
const path = require("path");
const moment = require("moment");
const xlsx = require("xlsx");
// const https = require("https"); // Comentado: SSL será manejado por Nginx
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

// Cargar certificados SSL - Comentado: SSL será manejado por Nginx
// const options = {
//     key: fs.readFileSync("/etc/letsencrypt/live/nwfg.net/privkey.pem"),
//     cert: fs.readFileSync("/etc/letsencrypt/live/nwfg.net/fullchain.pem")
// };

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

// Crear router para las rutas de upload con prefijo /api/upload
const uploadRouter = express.Router();

// 📌 Ruta para guardar las columnas seleccionadas en el paso 2
uploadRouter.post("/columns/selected", async (req, res) => {
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
uploadRouter.get("/columns/selected/:supplier", async (req, res) => {
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
uploadRouter.post("/file", upload.single("file"), async (req, res) => {
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
uploadRouter.get("/columns", async (req, res) => {

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
uploadRouter.post("/map-columns", async (req, res) => {
    let connection = null;
    
    try {
        const { supplier, columnMapping, rows, selectedColumns, headers } = req.body;

        if (!supplier || !columnMapping || !rows || rows.length === 0) {
            return res.json({ success: false, message: "Faltan datos necesarios", insertedRows: 0 });
        }

        // 💡 Validar filas válidas ANTES de conectar
        const validRows = rows.filter(row => {
            if (row.Rate !== undefined && row.Rate !== null) {
                let r = String(row.Rate).replace(",", ".");
                r = parseFloat(r);
                return !isNaN(r) && r >= 0;
            }
            return false;
        });

        if (validRows.length === 0) {
            return res.status(400).json({ success: false, message: "No se encontraron filas con Rate válido.", insertedRows: 0 });
        }

        const date = moment().format("YYYY-MM-DD");
        const time = moment().format("HH:mm:ss");
        const timestamp = moment().format("YYYYMMDD_HHmmss");
        const logDir = path.join(baseDir, "logs");
        await fs.ensureDir(logDir);

        // 1. Iniciar conexión
        connection = await mysql.createConnection(dbConfig);
        
        // 2. Backup de seguridad
        const backupTable = `Rates_backup_${timestamp}`;
        await connection.query(`CREATE TABLE ${backupTable} LIKE Rates`);
        await connection.query(`INSERT INTO ${backupTable} SELECT * FROM Rates`);
        
        // 3. Limpiar registros previos del proveedor
        await connection.query("DELETE FROM Rates WHERE SPL = ?", [supplier]);

        let dbColumns = Object.values(columnMapping);
        if (!dbColumns.includes("SPL")) dbColumns.push("SPL");

        // Lógica específica para Clean Sky (Extracción de meses)
        if (supplier === 'cs') {
            if (!dbColumns.includes("duracion_rate")) dbColumns.push("duracion_rate");
            for (const row of validRows) {
                if (row.Product_Name) {
                    const match = row.Product_Name.match(/\d+/);
                    row.duracion_rate = match ? parseInt(match[0], 10) : null;
                }
            }
        }

        // Definir columnas únicas y preparar la Query Robusta
        const uniqueDbColumns = [...new Set(dbColumns)];
        const placeholders = Array(uniqueDbColumns.length).fill("?").join(", ");
        
        // Crear la instrucción de actualización para duplicados
        const updateFields = uniqueDbColumns
            .filter(col => col !== 'Rate_ID') // No actualizamos la llave primaria
            .map(col => `${col} = VALUES(${col})`)
            .join(", ");

        const insertQuery = `
            INSERT INTO Rates (${uniqueDbColumns.join(", ")}) 
            VALUES (${placeholders})
            ON DUPLICATE KEY UPDATE ${updateFields}
        `;

        // 5. Inserción masiva con manejo de conflictos
        let insertedCount = 0;
        for (const row of validRows) {
            try {
                // Asegurar formato numérico para el Rate
                row.Rate = parseFloat(String(row.Rate).replace(",", "."));

                // Mapear valores siguiendo estrictamente el orden de uniqueDbColumns
                const values = uniqueDbColumns.map(col => {
                    if (col === "SPL") return row[col] || supplier;
                    return (row[col] !== undefined && row[col] !== null && row[col] !== "") ? row[col] : null;
                });

                await connection.query(insertQuery, values);
                insertedCount++;
            } catch (insertError) {
                // Aquí solo saltarán errores que no sean por duplicados (ej. falta de conexión)
                console.error(`❌ Error real de DB en fila #${insertedCount + 1}:`, insertError.message);
            }
        }

        res.json({
            success: true,
            message: `Datos procesados: ${insertedCount} filas insertadas.`,
            insertedRows: insertedCount
        });

    } catch (error) {
        console.error("❌ Error en /map-columns:", error);
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 📌 Ruta para obtener todas las filas del archivo subido
uploadRouter.get("/rows/:supplier", async (req, res) => {
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

// Montar el router de upload con el prefijo /api/upload
app.use("/api/upload", uploadRouter);

// 📌 Ruta para verificar el estado del servidor
app.get("/health", (req, res) => {
    res.json({ 
        status: "OK", 
        timestamp: new Date().toISOString(),
        version: "1.0.0" 
    });
});

// 📌 Manejador de errores
app.use((err, req, res, next) => {
    console.error("❌ Error no controlado:", err);
    logConsole.error("❌ Error no controlado:", err);
    res.status(500).json({ success: false, error: "Error interno del servidor" });
});

// 📌 Crear servidor HTTP (SSL será manejado por Nginx)
app.listen(PORT, "0.0.0.0", () => {
    const startupMessage = `🌐 Servidor HTTP corriendo en http://0.0.0.0:${PORT} - ${new Date().toISOString()}`;
    console.log(startupMessage);
    logConsole.log(startupMessage);
});