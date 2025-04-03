import express from "express";
import axios from "axios";
import session from "express-session";
import { BigQuery } from "@google-cloud/bigquery";
import cron from "node-cron";
import dotenv from "dotenv";

const app = express();
dotenv.config();
const lwClient = process.env.CLIENT;
const authorization = process.env.AUTH;
const accept = "application/json";

app.set("view engine", "ejs");
app.use(express.urlencoded({ extended: true }));
app.use(
  session({
    secret: "mySecretKey",
    resave: false,
    saveUninitialized: true,
  })
);

const bigquery = new BigQuery({
  projectId: "copia-bbdd-big-query",
  keyFilename: "./config/key.json",
});

app.use((req, res, next) => {
  console.log(`Request received: ${req.method} ${req.url}`);
  next();
});
function getTodayDate() {
  const today = new Date();
  const year = today.getFullYear();
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const day = String(today.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

async function obtenerAlumnos() {
  console.log("Iniciando la obtención de alumnos...");

  const headers = {
    "Lw-Client": lwClient,
    Authorization: authorization,
    Accept: accept,
  };

  const url = "https://academy.turiscool.com/admin/api/v2/users";
  let userLst = [];
  let page = 1;
  let hasMorePages = true;

  while (hasMorePages) {
    try {
      const response = await axios.get(url, {
        headers: headers,
        params: { page: page, items_per_page: "20" },
      });

      console.log(`Fetched page: ${page}`);
      if (response.data && response.data.data.length > 0) {
        response.data.data.forEach((user) => {
          try {
            const processedUser = {
              id: String(user.id),
              email: String(user.email),
              username: String(user.username),
              created: user.created, // Dejar como timestamp en segundos
              last_login: user.last_login ? user.last_login : null, // Dejar como timestamp en segundos o null
              tags: JSON.stringify(user.tags).replace(/"/g, "'"), // Reemplazar comillas dobles por simples
              nps_score:
                user.nps_score !== null ? user.nps_score.toFixed(1) : null, // Asegurar formato decimal
              nps_comment:
                user.nps_comment !== null ? String(user.nps_comment) : "None", // Si es null, cambiar a "None"
            };

            // Agregar a la lista de usuarios procesados
            userLst.push(processedUser);
          } catch (error) {
            console.error(`Error procesando usuario ID ${user.id}:`, error);
            // Continuar con el siguiente usuario
          }
        });

        const { totalPages } = response.data.meta;
        page += 1;
        hasMorePages = page <= totalPages;
      } else {
        hasMorePages = false;
        console.log("No more data to fetch.");
      }
    } catch (error) {
      console.error(
        `Error fetching page ${page}:`,
        error.response ? error.response.data : error
      );
      console.log("Intentando continuar con la siguiente página...");
      page += 1;
    }
  }

  console.log("Usuarios procesados:", userLst.length);
  return userLst;
}
async function obtenerCursos() {
  console.log("Iniciando la obtención de cursos...");
  const headers = {
    "Lw-Client": lwClient,
    Authorization: authorization,
    Accept: accept,
  };

  const url = "https://academy.turiscool.com/admin/api/v2/courses";
  let courseLst = [];
  let page = 1;
  let hasMorePages = true;

  while (hasMorePages) {
    try {
      const response = await axios.get(url, {
        headers: headers,
        params: { page: page, items_per_page: "20" },
      });

      console.log(`Fetched page: ${page}`);
      if (response.data && response.data.data.length > 0) {
        courseLst.push(...response.data.data);
        const { totalPages } = response.data.meta;

        page += 1;
        hasMorePages = page <= totalPages;
      } else {
        hasMorePages = false;
        console.log("No more data to fetch.");
      }
    } catch (error) {
      console.error(
        `Error fetching page ${page}:`,
        error.response ? error.response.data : error
      );
      hasMorePages = false;
    }
  }

  const courses = courseLst.map((course) => ({
    id: String(course.id),
    title: String(course.title),
    categories: JSON.stringify(course.categories),
    label: String(course.label || ""),
    created: String(course.created), // Timestamp en segundos
    modified: String(course.modified), // Timestamp en segundos
  }));

  console.log("Cursos procesados:", courses.length);
  return courses;
}
async function obtenerProgreso(alumnos) {
  console.log("Iniciando la obtención de progreso...");
  const startTime = new Date();
  const headers = {
    "Lw-Client": lwClient,
    Authorization: authorization,
    Accept: accept,
  };

  const users = alumnos;
  const userMap = new Map();
  users.forEach((user) => {
    userMap.set(user.id, user);
  });

  for (const userId of userMap.keys()) {
    let currentPage = 1;
    let totalPages = 1;
    let allProgressData = [];

    try {
      while (currentPage <= totalPages) {
        const url = `https://academy.turiscool.com/admin/api/v2/users/${userId}/progress`;
        const response = await axios.get(url, {
          headers: headers,
          params: {
            items_per_page: 150,
            page: currentPage,
          },
        });

        const progressArray = response.data.data || [];

        if (progressArray.length > 0) {
          console.log(
            `Progreso encontrado para el usuario ${userId}, página ${currentPage}.`
          );

          const progressData = progressArray.map((progress) => {
            const user = userMap.get(userId);
            return {
              course_id: String(progress.course_id),
              status: String(progress.status),
              progress_rate: String(progress.progress_rate),
              average_score_rate: String(progress.average_score_rate),
              time_on_course: String(progress.time_on_course),
              total_units: String(progress.total_units),
              completed_units: String(progress.completed_units),
              id: String(userId),
              email: String(user.email),
              username: String(user.username),
              created: String(user.created),
              last_login: String(user.last_login),
              tags: String(user.tags),
              nps_score: String(user.nps_score || ""),
              nps_comment: String(user.nps_comment || ""),
              fecha: new Date().toISOString().split("T")[0],
            };
          });

          allProgressData.push(...progressData);

          // Actualizar la paginación
          totalPages = response.data.pagination.total_pages;
          currentPage++;
        } else {
          console.log(`No se encontró progreso para el usuario ${userId}.`);
          break;
        }
      }

      if (allProgressData.length > 0) {
        await guardarProgresoEnBigQuery(allProgressData);
        console.log(`Progreso guardado en BigQuery para el usuario ${userId}.`);
        await guardarProgresoEnBigQueryAcumulado(allProgressData);
        console.log(
          `Progreso acumulado guardado en BigQuery para el usuario ${userId}.`
        );
      }
    } catch (error) {
      if (
        error.response &&
        error.response.data &&
        error.response.data.error === "Progress data not found"
      ) {
        console.warn(
          `Progreso no encontrado para el usuario ${userId}: ${error.response.data.error}`
        );
      } else {
        console.error(
          `Error al obtener el progreso del usuario ${userId}:`,
          error.response ? error.response.data : error
        );
      }
    }
  }

  const endTime = new Date();
  const timeDiff = (endTime - startTime) / 1000;
  console.log(
    `Proceso de obtención de progreso finalizado. Tiempo total: ${timeDiff} segundos.`
  );
}
async function verificarBigQuery() {
  const query = `SELECT 1 as test`;
  try {
    const [rows] = await bigquery.query({ query });
    console.log("Conexión a BigQuery exitosa:", rows);
  } catch (error) {
    console.error("Error en la conexión a BigQuery:", error);
  }
}
async function vaciarTablaAlumnos() {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaAlumnos";

  const query = `TRUNCATE TABLE \`${datasetId}.${tableId}\``;

  try {
    await bigquery.query({ query });
    console.log("Tabla de alumnos vaciada correctamente.");
  } catch (error) {
    console.error("Error al vaciar la tabla de alumnos:", error);
  }
}
async function vaciarTablaProgreso() {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaProgreso";

  const query = `TRUNCATE TABLE \`${datasetId}.${tableId}\``;

  try {
    await bigquery.query({ query });
    console.log("Tabla de progreso vaciada correctamente.");
  } catch (error) {
    console.error("Error al vaciar la tabla de progreso:", error);
  }
}
async function guardarAlumnosEnBigQuery(alumnos) {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaAlumnos";

  const rows = alumnos.map((alumno) => ({
    id: String(alumno.id),
    email: String(alumno.email),
    username: String(alumno.username),
    created: alumno.created, // Dejar el timestamp en segundos como número
    // Dejar el timestamp como número o null
    last_login: alumno.last_login ? alumno.last_login : null,
    tags: String(alumno.tags),
    nps_score: String(alumno.nps_score) ? alumno.nps_score : null,
    nps_comment: String(alumno.nps_comment || ""),
  }));

  try {
    await bigquery.dataset(datasetId).table(tableId).insert(rows);
    console.log(`Datos insertados correctamente en la tabla de BigQuery.`);
  } catch (error) {
    console.error("Error al insertar datos en BigQuery:", error);
  }
}
async function guardarCursosEnBigQuery(cursos) {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaCursos";

  const rows = cursos.map((curso) => ({
    id: String(curso.id),
    title: String(curso.title),
    categories: String(curso.categories),
    label: String(curso.label || ""),
    created: String(curso.created), // Timestamp en segundos
    modified: String(curso.modified), // Timestamp en segundos
  }));

  try {
    await bigquery.dataset(datasetId).table(tableId).insert(rows);
    console.log(`Datos insertados correctamente en la tabla de BigQuery.`);
  } catch (error) {
    console.error("Error al insertar datos en BigQuery:", error);
  }
}
async function guardarProgresoEnBigQuery(progreso) {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaProgreso";

  const rows = progreso.map((prog) => ({
    course_id: String(prog.course_id),
    username: String(prog.username),
    status: String(prog.status),
    progress_rate: String(prog.progress_rate),
    average_score_rate: String(prog.average_score_rate),
    time_on_course: String(prog.time_on_course),
    total_units: String(prog.total_units),
    completed_units: String(prog.completed_units),
    fecha: String(prog.fecha),
    email: String(prog.email),
    created: prog.created, // Mantener el valor en formato numérico (segundos desde 1970)
    last_login: prog.last_login ? prog.last_login : null, // Mantener el valor en formato numérico o null
    tags: String(prog.tags),
    nps_score: String(prog.nps_score || ""),
    nps_comment: String(prog.nps_comment || ""),
    id: String(prog.id),
  }));

  const maxBatchSize = 10000;
  let batchCount = 0;

  for (let i = 0; i < rows.length; i += maxBatchSize) {
    const batch = rows.slice(i, i + maxBatchSize);

    try {
      await bigquery.dataset(datasetId).table(tableId).insert(batch);
      console.log(`Lote ${batchCount + 1} insertado correctamente.`);
    } catch (error) {
      console.error(`Error al insertar el lote ${batchCount + 1}:`, error);
    }

    batchCount++;
  }

  console.log(`Todos los lotes han sido insertados en la tabla de BigQuery.`);
}
async function eliminarAlumnosDuplicadosBigQuery() {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaAlumnos";
  const tempTableId = "temp_tablaAlumnos";

  const queryCreateTempTable = `
    CREATE OR REPLACE TABLE \`${datasetId}.${tempTableId}\` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created DESC) AS rn
      FROM \`${datasetId}.${tableId}\`
    )
    WHERE rn = 1;
  `;

  const queryReplaceOriginalTable = `
    CREATE OR REPLACE TABLE \`${datasetId}.${tableId}\` AS
    SELECT * FROM \`${datasetId}.${tempTableId}\`;
  `;

  try {
    await bigquery.query({ query: queryCreateTempTable });
    console.log("Tabla temporal creada con datos únicos para alumnos.");

    await bigquery.query({ query: queryReplaceOriginalTable });
    console.log("Tabla de alumnos reemplazada con datos únicos.");
  } catch (error) {
    console.error(
      "Error al eliminar registros duplicados en la tabla alumnos:",
      error
    );
  }
}
async function guardarProgresoEnBigQueryAcumulado(progreso) {
  //crear tabla tablaProgresoAcumulado
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaProgresoAcumulado";

  const rows = progreso.map((prog) => ({
    course_id: String(prog.course_id),
    username: String(prog.username),
    status: String(prog.status),
    progress_rate: String(prog.progress_rate),
    average_score_rate: String(prog.average_score_rate),
    time_on_course: String(prog.time_on_course),
    total_units: String(prog.total_units),
    completed_units: String(prog.completed_units),
    fecha: String(prog.fecha),
    email: String(prog.email),
    created: prog.created, // Mantener el valor en formato numérico (segundos desde 1970)
    last_login: prog.last_login ? prog.last_login : null, // Mantener el valor en formato numérico o null
    tags: String(prog.tags),
    nps_score: String(prog.nps_score || ""),
    nps_comment: String(prog.nps_comment || ""),
    id: String(prog.id),
  }));

  const maxBatchSize = 10000;
  let batchCount = 0;

  for (let i = 0; i < rows.length; i += maxBatchSize) {
    const batch = rows.slice(i, i + maxBatchSize);

    try {
      await bigquery.dataset(datasetId).table(tableId).insert(batch);
      console.log(`Lote ${batchCount + 1} insertado correctamente.`);
    } catch (error) {
      console.error(`Error al insertar el lote ${batchCount + 1}:`, error);
    }

    batchCount++;
  }

  console.log(`Todos los lotes han sido insertados en la tabla de BigQuery.`);
}
async function eliminarCursosDuplicadosBigQuery() {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaCursos";
  const tempTableId = "temp_tablaCursos";

  const queryCreateTempTable = `
    CREATE OR REPLACE TABLE \`${datasetId}.${tempTableId}\` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created DESC) AS rn
      FROM \`${datasetId}.${tableId}\`
    )
    WHERE rn = 1;
  `;

  const queryReplaceOriginalTable = `
    CREATE OR REPLACE TABLE \`${datasetId}.${tableId}\` AS
    SELECT * FROM \`${datasetId}.${tempTableId}\`;
  `;

  try {
    await bigquery.query({ query: queryCreateTempTable });
    console.log("Tabla temporal creada con datos únicos para cursos.");

    await bigquery.query({ query: queryReplaceOriginalTable });
    console.log("Tabla de cursos reemplazada con datos únicos.");
  } catch (error) {
    console.error(
      "Error al eliminar registros duplicados en la tabla cursos:",
      error
    );
  }
}
async function eliminarProgresoDuplicadoBigQuery() {
  const datasetId = "copia_bbdd_bq";
  const tableId = "tablaProgreso";
  const tempTableId = "temp_tablaProgreso";

  const queryCreateTempTable = `
    CREATE OR REPLACE TABLE \`${datasetId}.${tempTableId}\` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY course_id, username ORDER BY created DESC) AS rn
      FROM \`${datasetId}.${tableId}\`
    )
    WHERE rn = 1;
  `;

  const queryReplaceOriginalTable = `
    CREATE OR REPLACE TABLE \`${datasetId}.${tableId}\` AS
    SELECT * FROM \`${datasetId}.${tempTableId}\`;
  `;

  try {
    await bigquery.query({ query: queryCreateTempTable });
    console.log("Tabla temporal creada con datos únicos para progreso.");

    await bigquery.query({ query: queryReplaceOriginalTable });
    console.log("Tabla de progreso reemplazada con datos únicos.");
  } catch (error) {
    console.error(
      "Error al eliminar registros duplicados en la tabla progreso:",
      error
    );
  }
}
async function start() {
  try {
    getTodayDate();
    //await crearTablaUsuariosDelDia();
    //await crearTablaProgresoDelDia();
    await vaciarTablaAlumnos();
    await vaciarTablaProgreso();
    const cursos = await obtenerCursos();
    const alumnos = await obtenerAlumnos();
    await guardarAlumnosEnBigQuery(alumnos);
    await eliminarAlumnosDuplicadosBigQuery();
    await guardarCursosEnBigQuery(cursos);
    await eliminarCursosDuplicadosBigQuery();
    await obtenerProgreso(alumnos);
    await eliminarProgresoDuplicadoBigQuery();
    //await copiarContenidoTablaAlumnosEnTablaAlumnosDelDia();
    //await copiarContenidoTablaProgresoEnTablaProgresoDelDia();
  } catch (error) {
    console.error("Error:", error);
  }
}

app.get("/start", async (req, res) => {
  try {
    await start();
    res.send("Proceso de sincronización completado.");
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error al sincronizar los datos.");
  }
});

//rutas para llamar a lw
app.get("/obtener-alumnos", async (req, res) => {
  try {
    await vaciarTablaAlumnos();
    const alumnos = await obtenerAlumnos();
    await guardarAlumnosEnBigQuery(alumnos);
    await eliminarAlumnosDuplicadosBigQuery();
    res.json(alumnos);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error al obtener los alumnos.");
  }
});
app.get("/obtener-cursos", async (req, res) => {
  try {
    const cursos = await obtenerCursos();
    await guardarCursosEnBigQuery(cursos);
    await eliminarCursosDuplicadosBigQuery();
    res.json(cursos);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error al obtener los cursos.");
  }
});
app.get("/obtener-progreso", async (req, res) => {
  await vaciarTablaProgreso();
  try {
    const alumnos = await obtenerAlumnos();
    await obtenerProgreso(alumnos);
    await eliminarProgresoDuplicadoBigQuery();
    res.send("Progreso obtenido correctamente.");
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error al obtener el progreso.");
  }
});
//rutas de API
app.get("/api/alumnos", async (req, res) => {
  try {
    const query = `SELECT * FROM copia_bbdd_bq.tablaAlumnos`;
    const [rows] = await bigquery.query({ query });
    res.json(rows);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error al obtener los alumnos.");
  }
});
app.get("/api/cursos", async (req, res) => {
  try {
    const query = `SELECT * FROM copia_bbdd_bq.tablaCursos`;
    const [rows] = await bigquery.query({ query });
    res.json(rows);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error al obtener los cursos.");
  }
});
app.get("/api/progreso", async (req, res) => {
  try {
    const query = `SELECT * FROM copia_bbdd_bq.tablaProgreso`;
    const [rows] = await bigquery.query({ query });
    res.json(rows);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error al obtener el progreso.");
  }
});

cron.schedule("0 2 * * *", async () => {
  console.log("Iniciando la sincronización programada...");
  await start();
  console.log("Sincronización programada completada.");
});
verificarBigQuery();
app.listen(8080, () => {
  console.log("Server is running on 8080");
});
