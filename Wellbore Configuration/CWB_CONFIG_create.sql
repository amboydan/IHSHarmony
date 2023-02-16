USE [Gas_Forecasting_Sandbox]
GO

/****** Object:  Table [dbo].[WELL_INFORMATION]    Script Date: 2/9/2023 11:39:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CWB_CONFIG](
	[WELL_KEY] [varchar](255) NULL,
	[WB_CONFIG_ID] numeric NULL,
	[DATE_TIME] datetime NULL,
	[CONFIG_NAME] [varchar](255) NULL,
	[FLOW_PATH] numeric NULL,
	[DATUM_MD_DEPTH] numeric NULL,
	[WELLHEAD_TEMPERATURE] numeric NULL,
	[SANDFACE_TEMPERATURE] numeric NULL
) ON [PRIMARY]
GO


