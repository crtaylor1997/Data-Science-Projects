VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Sheet11"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = True

Sub GetData()

Dim Datawb As Workbook
Dim ASRwb As Workbook
Dim LoanName As String
Dim SourceTable As Range
Dim LoanCell As Range
Dim DataPath$, DataSheet$
Dim result As Range
Dim resultRow As Range
Dim TargetRange As Range
Dim LookUpCol, LookUpRow As Range
Dim OffsetCountCol, OffsetCountRow As Long
Dim Dict
Dim varKey As Variant
Dim SearchRange() As Variant
Dim index As Integer
Set Dict = CreateObject("Scripting.Dictionary")
Dict.Add "Loan", "A2"
Dict.Add "Property", "A4"

Set ASRwb = ThisWorkbook
Application.ScreenUpdating = False
DataPath = "S:\DealCloud\Data Export\"
DataSheet = "Export Data"
Set LoanCell = ASRwb.Sheets("Deal Overview").Range("B3")
LoanName = LoanCell.Value


For Each varKey In Dict.Keys()
    Set Datawb = Workbooks.Open(DataPath & varKey & ".xlsx")
    Set LookUpCol = (Sheets(DataSheet).Range("A1").EntireColumn)
    Set LookUpRow = (Sheets(DataSheet).Range("A1").EntireRow)
    OffsetCountRow = Application.WorksheetFunction.CountA(LookUpRow)
    OffsetCountCol = Application.WorksheetFunction.CountA(LookUpCol) - 1
    Dim resultArray() As Variant
    Application.FindFormat.Clear
    Datawb.Sheets(DataSheet).Unprotect
    Set SourceTable = Datawb.Sheets(DataSheet).Range("A1").Offset(OffsetCountCol, OffsetCountRow)
    SearchRange = SourceTable.Worksheet.UsedRange.Value2
    For i = LBound(SearchRange, 1) To UBound(SearchRange, 1)
        For j = LBound(SearchRange, 2) To UBound(SearchRange, 2)
            If SearchRange(i, j) = LoanName Then index = i
        Next j
    Next i
 
    resultArray = Application.WorksheetFunction.index(SearchRange, index, 0)
    Datawb.Close SaveChanges:=False
   
    Set TargetRange = ASRwb.Sheets("Data").Range(Dict(varKey)).EntireRow
    TargetRange = resultArray
Next

Application.ScreenUpdating = True


End Sub
