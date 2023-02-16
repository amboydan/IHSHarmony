VERSION 5.00
Begin {C62A69F0-16DC-11CE-9E98-00AA00574A4F} createConfig 
   Caption         =   "Create Well Bore Configuration"
   ClientHeight    =   4755
   ClientLeft      =   120
   ClientTop       =   465
   ClientWidth     =   6960
   OleObjectBlob   =   "createConfig.frx":0000
   ShowModal       =   0   'False
   StartUpPosition =   2  'CenterScreen
End
Attribute VB_Name = "createConfig"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = False

Private Sub cwb_cancel_Click()
    Unload createConfig
End Sub

Private Sub cwb_submit_Click()

Dim Conn As New ADODB.Connection
Dim recset As New ADODB.Recordset
Dim sqlQry As String, sConnect As String
Dim RowCount As Integer
Application.ScreenUpdating = False

    If configName.Value = "" Then
        MsgBox "Configuration Name?"
        GoTo DoNotSubmit
    End If
    If Not IsNumeric(datumDepth.Value) Then
        MsgBox "Datum Depth must be Numeric!"
        GoTo DoNotSubmit
    End If
    If Not IsNumeric(whTemp.Value) Then
        MsgBox "Wellhead Temp must be Numeric!"
        GoTo DoNotSubmit
    End If
    If Not IsNumeric(sfTemp.Value) Then
        MsgBox "Sand Face Temp must be Numeric!"
        GoTo DoNotSubmit
    End If

    sqlInsert_config = "INSERT INTO CWB_CONFIG (WELL_KEY, WB_CONFIG_ID, DATE_TIME, " & _
        "CONFIG_NAME, FLOW_PATH, DATUM_MD_DEPTH, WELLHEAD_TEMPERATURE, SANDFACE_TEMPERATURE) " & _
        "VALUES ('" + wellKey.Caption + "','" + configNum.Caption + "','" & _
        Format(configDate.Value, "yyyy-mm-dd") + "','" & _
        configName.Value + "'," + Format(flowPath.ListIndex, "0") + "," + datumDepth.Value + "," & _
        whTemp.Value + "," + sfTemp.Value + ")"
        
        
    'MsgBox (sqlInsert_config)
    sConnect = "Driver={SQL Server};Server=ancsql04; Database=Gas_Forecasting_Sandbox;" & _
                "Trusted_Connection=yes;"
    Conn.Open sConnect
    Set recset = New ADODB.Recordset
        recset.Open sqlInsert_config, Conn
        'recset.Close
    Conn.Close
    
    Unload createConfig
    createConfig.Hide
    
DoNotSubmit:
End Sub

Private Sub Label5_Click()

End Sub

Private Sub UserForm_Initialize()
'Well Name
    wellName.Caption = Worksheets("Well Selection").Range("A8").Value
'Well Key
    wellKey.Caption = Worksheets("Well Selection").Range("A11").Value
'Config Number
    configNum.Caption = Worksheets("Well Selection").Range("C11").Value + 1
'Config Name
    configName.MaxLength = 20
'Date
    configDate.Value = Date
'Flow Path
    Call flowPath.AddItem("Tubing", 0)
    Call flowPath.AddItem("Annulus", 1)
    Call flowPath.AddItem("Both", 2)
    Call flowPath.AddItem("Casing", 3)
    flowPath.ListIndex = 0
'Datum Depth
'Well Head Temp
'Sand Face Temp
End Sub
