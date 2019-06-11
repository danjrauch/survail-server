const throng = require('throng')
const Queue = require('bull')
const shell = require('shelljs')
const { Pool } = require('pg')

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
const POSTGRES_URL = process.env.DATABASE_URL || 'postgres://127.0.0.1:5432/survail'

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
const workers = process.env.WEB_CONCURRENCY || 2

// The maxium number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
const maxJobsPerWorker = 50

const pool = new Pool({
  connectionString: POSTGRES_URL,
  ssl: POSTGRES_URL == process.env.DATABASE_URL ? true : false
})

function sleep(ms){
  return new Promise(resolve => setTimeout(resolve, ms))
}

function start(){
  // Connect to the named work queue
  let workQueue = new Queue('work', REDIS_URL)

  workQueue.process(maxJobsPerWorker, async (job) => {
    // const client = new Client()
    // This is an example job that just slowly reports on progress
    // while doing no work. Replace this with your own job logic.
    let progress = 0
    let ret = {}

    // throw an error 5% of the time
    // if(Math.random() < 0.05){
    //   throw new Error('This job failed!')
    // }
    const type = job.data.type
    const args = job.data.args

    if(type == 'add'){
      try{
        const query = `INSERT into task (name, description, due_time, created_on) values ($$${args.name}$$, $$${args.description ? args.description : ''}$$, now() + interval '${args.offset ? args.offset : '0'} days', now())`
        const res = await pool.query(query)
        ret = {code: 0, result: res}
      }catch(err){
        ret = {code: 1}
        console.log(err)
      }
      job.progress(100)
      return ret
    }

    if(type == 'delete'){
      try{
        const ids = args.ids.reduce((str, curr) => str + `'${curr}',`, '')
        const res = await pool.query(`DELETE from task where id in (${ids.slice(0, ids.length-1)})`)
        ret = {code: 0, result: res}
      }catch(err){
        ret = {code: 1}
        console.log(err)
      }
      job.progress(100)
      return ret
    }

    if(type == 'hub'){
      try{
        const tasksDueToday = await pool.query(`SELECT id, name, description, due_time FROM task WHERE due_time::date = now()::date`)
        const tasksDueTomorrow = await pool.query(`SELECT id, name, description, due_time FROM task WHERE due_time::date > now()::date and due_time::date <= now()::date + '1 day'::interval`)
        ret = {code: 0, tasksDueToday: tasksDueToday.rows, tasksDueTomorrow: tasksDueTomorrow.rows}
      }catch(err){
        ret = {code: 1}
        console.log(err)
      }
      job.progress(100)
      return ret
    }

    if(type == 'list'){
      try{
        const res = await pool.query('SELECT id, name, due_time, description, to_char(due_time, $$FMDay$$) as dayofweek, due_time::date - now()::date as daysbetween FROM task ORDER BY daysbetween')
        ret = {code: 0, tasks: res.rows}
      }catch(err){
        ret = {code: 1}
        console.log(err)
      }
      job.progress(100)
      return ret
    }

    if(type == 'update'){
      let assignments = ''
      Object.keys(args).filter(a => a != 'id').forEach((a, i) => {
        if(a == 'name' || a == 'n'){
          if(i == 0) assignments += `name = $$${args.name}$$`
          else assignments += `, name = $$${args.name}$$`
        }
        else if(a == 'description' || a == 'd'){
          if(i == 0) assignments += `description = $$${args.description}$$`
          else assignments += `, description = $$${args.description}$$`
        }
        else if(a == 'offset' || a == 'o'){
          if(i == 0) assignments += `due_time = now() + interval '$$${args.offset}$$ days'`
          else assignments += `, due_time = now() + interval '$$${args.offset}$$ days'`
        }
      })

      if(assignments){
        try{
          const res = await pool.query(`UPDATE task SET ${assignments} WHERE id = $$${args.id}$$`)
          ret = {code: 0, result: res}
        }catch(err){
          ret = {code: 1}
          console.log(err)
        }
      }else{
        ret = {code: 1}
        console.log('You need to specify assignments.')
      }
      job.progress(100)
      return ret
    }

    // while(progress < 100){
    //   await sleep(50)
    //   progress += 50
    //   job.progress(progress)
    // }

    await pool.end()

    // A job can return values that will be stored in Redis as JSON
    // This return value is unused in this demo application.
    return { value: 'Error' }
  })
}

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start })