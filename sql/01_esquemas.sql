-- ---------------------------------------------------------------------------
-- Base analitica del proyecto.
--
-- NO ES UNA CONFIGURACION PRODUCTIVA: sin TLS, sin gestion de secretos, sin
-- respaldos. Solo para el entorno local del proyecto academico.
--
-- Este archivo se monta en /docker-entrypoint-initdb.d/ del contenedor de
-- MySQL, asi que se ejecuta UNA SOLA VEZ, cuando el volumen de datos esta
-- vacio. Si se modifica despues de haber levantado el entorno, hay que hacer
-- `docker compose down -v` para que vuelva a correr.
--
-- Los archivos de esta carpeta son la UNICA fuente de verdad del esquema. El
-- codigo Python no crea tablas: solo verifica que existan. Tener el DDL en dos
-- lugares garantiza que tarde o temprano divergan.
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS cripto
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

-- utf8mb4 y no utf8: en MySQL `utf8` ocupa 3 bytes y no cubre todo Unicode.
-- Aqui casi todo es numerico, pero los nombres de activos del catalogo pueden
-- traer caracteres fuera del plano basico.

USE cripto;
