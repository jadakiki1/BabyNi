using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;

namespace BabyNi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataController : Controller
    {
        DataAggregationService dataAggregationService = new DataAggregationService();

        [HttpGet("hourly/{aggType}")]
        public ActionResult<List<AggregatedDataDto>> GetHourlyData(string aggType)
        {
            if (aggType != "NeAlias" && aggType != "NeType")
            {
                return BadRequest("Invalid aggregation type. Please use 'NeAlias' or 'NeType'.");
            }

            var data = dataAggregationService.GetAggregatedData("TRANS_MW_AGG_SLOT_HOURLY", aggType);
            return Ok(data);
        }

        [HttpGet("daily/{aggType}")]
        public ActionResult<List<AggregatedDataDto>> GetDailyData(string aggType)
        {
            if (aggType != "NeAlias" && aggType != "NeType")
            {
                return BadRequest("Invalid aggregation type. Please use 'NeAlias' or 'NeType'.");
            }

            var data = dataAggregationService.GetAggregatedData("TRANS_MW_AGG_SLOT_DAILY", aggType);
            return Ok(data);
        }
    }
}
