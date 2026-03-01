$response = Invoke-WebRequest -Uri 'http://localhost:8000/api/financial-metrics/4322/data-by-section' -UseBasicParsing
$data = $response.Content | ConvertFrom-Json

Write-Host "Available Periods:"
foreach ($period in ($data | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name | Sort-Object)) {
    Write-Host "  - $period"
}

$annualData = $data.'2024 Annual'
Write-Host "`nSections in 2024 Annual:"
foreach ($section in ($annualData | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name | Sort-Object)) {
    $count = ($annualData.$section | Measure-Object).Count
    Write-Host "  - $section : $count metrics"
}

Write-Host "`nTotal metrics per section:"
$total = 0
foreach ($section in ($annualData | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name | Sort-Object)) {
    $count = ($annualData.$section | Measure-Object).Count
    $total += $count
}
Write-Host "Total: $total"
