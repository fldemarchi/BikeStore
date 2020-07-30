/*
--------------------------------------------------------------------
© 2017 sqlservertutorial.net All Rights Reserved
--------------------------------------------------------------------
Name   : BikeStores
Link   : http://www.sqlservertutorial.net/load-sample-database/
Version: 1.0
Data Marts
--------------------------------------------------------------------
*/
-- create dimensions
USE [BikeStores]
GO
/****** Object:  Table [bi].[dim_brands]    Script Date: 29/7/2020 20:04:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bi].[dim_brands](
	[brand_id] [int]  NOT NULL,
	[brand_name] [varchar](255) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [bi].[dim_categories]    Script Date: 29/7/2020 20:04:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bi].[dim_categories](
	[category_id] [int] NOT NULL,
	[category_name] [varchar](255) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [bi].[dim_customers]    Script Date: 29/7/2020 20:04:25 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bi].[dim_customers](
	[customer_id] [int] NOT NULL,
	[first_name] [varchar](255) NOT NULL,
	[last_name] [varchar](255) NOT NULL,
	[phone] [varchar](25) NULL,
	[email] [varchar](255) NOT NULL,
	[street] [varchar](255) NULL,
	[city] [varchar](50) NULL,
	[state] [varchar](25) NULL,
	[zip_code] [varchar](5) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [bi].[dim_products]    Script Date: 29/7/2020 20:04:25 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bi].[dim_products](
	[product_id] [int] NOT NULL,
	[product_name] [varchar](255) NOT NULL,
	[model_year] [smallint] NOT NULL,
	[list_price] [decimal](10, 2) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [bi].[dim_staffs]    Script Date: 29/7/2020 20:04:26 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bi].[dim_staffs](
	[staff_id] [int] NOT NULL,
	[first_name] [varchar](50) NOT NULL,
	[last_name] [varchar](50) NOT NULL,
	[email] [varchar](255) NOT NULL,
	[phone] [varchar](25) NULL,
	[active] [tinyint] NOT NULL,
	[store_id] [int] NOT NULL,
	[manager_id] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [bi].[dim_stores]    Script Date: 29/7/2020 20:04:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bi].[dim_stores](
	[store_id] [int] NOT NULL,
	[store_name] [varchar](255) NOT NULL,
	[phone] [varchar](25) NULL,
	[email] [varchar](255) NULL,
	[street] [varchar](255) NULL,
	[city] [varchar](255) NULL,
	[state] [varchar](10) NULL,
	[zip_code] [varchar](5) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [bi].[fact_orders]    Script Date: 29/7/2020 20:04:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [bi].[fact_orders](
	[order_id] [int] NOT NULL,
	[customer_id] [int] NOT NULL,
	[store_id] [int] NOT NULL,
	[staff_id] [int] NOT NULL,
	[item_id] [int] NOT NULL,
	[manager_id] [int] NULL,
	[product_id] [int] NOT NULL,
	[brand_id] [int] NOT NULL,
	[category_id] [int] NOT NULL,
	[order_date_id] [nvarchar](4000) NULL,
	[required_date_id] [nvarchar](4000) NULL,
	[shipped_date_id] [nvarchar](4000) NULL,
	[quantity] [int] NOT NULL,
	[list_price] [decimal](10, 2) NOT NULL,
	[discount] [decimal](4, 2) NOT NULL
) ON [PRIMARY]
GO
